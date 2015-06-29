'use strict';
class FlowletDetailInputController {
  constructor($state, $scope, MyDataSource, MyMetricsQueryHelper, myFlowsApi, myAlert) {
    this.MyDataSource = MyDataSource;
    this.$scope = $scope;
    this.$state = $state;
    this.runsCtrl = $scope.RunsController;
    this.MyMetricsQueryHelper = MyMetricsQueryHelper;
    this.flowletid = $scope.FlowletsController.activeFlowlet.name;

    this.inputs = [];

    let params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi
      .get(params)
      .$promise
      .then(res => {
        // INPUTS
        angular.forEach(res.connections, v => {
          if (v.targetName === this.flowletid) {
            this.inputs.push({
              name: v.sourceName,
              max: 0,
              type: v.sourceType
            });
          }
        });

        if (this.inputs.length > 0) {
          this.formatInput();
        }
      });
  }

  pollArrivalRate(input) {
    let arrivalPath = `/metrics/query?metric=system.process.events.processed\
                        &tag=namespace:${this.$state.params.namespace}\
                        &tag=app:${this.$state.params.appId}\
                        &tag=flow${this.$state.params.programId}\
                        &tag=flowlet:${this.flowletid}\
                        &tag=run:${this.runsCtrl.runs.selected.runid}\
                        &start=now-1s&end=now`;
    let dataSrc = new this.MyDataSource(this.$scope);
    // TODO: should this value be averaged over more than just the past 1 second?
    // POLLING ARRIVAL RATE
    dataSrc
      .poll({
        _cdapPath: arrivalPath,
        method: 'POST'
      }, res => {
        if (res.series[0]) {
         input.total = res.series[0].data[0].value;
        }
      });
  }

  formatInput() {
    angular.forEach(this.inputs, input => {
      let dataSrc = new this.MyDataSource(this.$scope);
      let flowletTags = {
        namespace: this.$state.params.namespace,
        app: this.$state.params.appId,
        flow: this.$state.params.programId,
        consumer: this.flowletid,
        producer: input.name
      };
      let tagsToParams = this.MyMetricsQueryHelper.tagsToParams(flowletTags);

      let path = `/metrics/query?${tagsToParams}&metric=system.queue.pending`;

      let aggregate = 0;
      // Get Aggregate
      dataSrc
        .request({
          _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
          method: 'POST'
        })
        .then( res => {
          // Get initial aggregate
          aggregate = res.series[0] ? res.series[0].data[0].value : 0;

          // Get timeseries
          return dataSrc.request({
            _cdapPath: path + '&start=now-60s&end=now',
            method: 'POST'
          });
        })
        .then( initial => {
          this.formatInitialTimeseries(aggregate, initial, input);

          // start polling aggregate
          this.pollAggregateQueue(path, input);
        });

      this.pollArrivalRate(input);

    });
  }

  pollAggregateQueue(path, input) {
    let dataSrc = new this.MyDataSource(this.$scope);

    dataSrc.poll({
      _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
      method: 'POST',
      interval: 1000
    }, streamData => {
      let stream = {
        time: Math.floor((new Date()).getTime()/1000),
        y: 0
      };
      let array;
      if (streamData.series[0]) {
        stream.y = streamData.series[0].data[0].value;
      }

      array = input.history[0].values;
      array.shift();
      array.push(stream);

      input.history = [
        {
          label: 'output',
          values: array
        }
      ];

      input.stream = array.slice(-1);
      input.max = Math.max.apply(Math, array.map(o => o.y));

    });
  }

  formatInitialTimeseries(aggregate, initial, input) {
    let v = [];

    if (initial.series[0]) {
      let response = initial.series[0].data;
      response[response.length - 1].value = aggregate - response[response.length - 1].value;
      for (let i = response.length - 2; i >= 0; i--) {
        response[i].value = response[i+1].value - response[i].value;
        v.unshift({
          time: response[i].time,
          y: response[i].value
        });
      }
    } else {
      // when there is no data
      for (let i = 60; i > 0; i--) {
        v.push({
          time: Math.floor((new Date()).getTime()/1000 - (i)),
          y: 0
        });
      }
    }

    input.stream = v.slice(-1);

    input.history = [{
      label: 'output',
      values: v
    }];
  }
}
FlowletDetailInputController.$inject = ['$state', '$scope', 'MyDataSource', 'MyMetricsQueryHelper', 'myFlowsApi', 'myAlert'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailInputController', FlowletDetailInputController);
