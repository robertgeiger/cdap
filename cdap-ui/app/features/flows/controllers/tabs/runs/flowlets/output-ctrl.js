'use strict';
class FlowletDetailOutputController {
  constructor($state, $scope, MyDataSource, MyMetricsQueryHelper, myFlowsApi) {
    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.FlowletsController.activeFlowlet.name;
    var runid = $scope.RunsController.runs.selected.runid;
    this.outputs = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    var flowletTags = {
      namespace: $state.params.namespace,
      app: $state.params.appId,
      flow: $state.params.programId,
      run: runid,
      flowlet: flowletid
    };

    myFlowsApi
      .get(params)
      .$promise
      .then(res => {

        // OUTPUTS
        angular.forEach(res.connections, v => {
          if (v.sourceName === flowletid) {
            this.outputs.push(v.targetName);
          }
        });

        if (this.outputs.length > 0) {
          let tagsToParams = MyMetricsQueryHelper.tagsToParams(flowletTags);
          // OUTPUT METRICS
          dataSrc
            .poll({
              _cdapPath: `/metrics/query?${tagsToParams}&metric=system.process.events.out&start=now-60s&count=60`,
              method: 'POST'
            }, res => this.updateOutput(res));

          // Total
          dataSrc
            .poll({
              _cdapPath: `/metrics/query?${tagsToParams}&metric=system.process.events.out`,
              method: 'POST'
            }, res => {
              if (res.series[0]) {
                this.total = res.series[0].data[0].value;
              }
            });
        }
      });
  }

  updateOutput(res) {
    let v = [];
    if (res.series[0]) {
      angular.forEach( res.series[0].data, val => v.push({ time: val.time, y: val.value }) );
    } else {
      for (let i = 60; i > 0; i--) {
        v.push({
          time: Math.floor((new Date()).getTime()/1000 - (i)),
          y: 0
        });
      }
    }

    if (this.outputHistory) {
      this.outputStream = v.slice(-1);
    }

    this.outputHistory = [{
      label: 'output',
      values: v
    }];

  }
}
FlowletDetailOutputController.$inject = ['$state', '$scope', 'MyDataSource', 'MyMetricsQueryHelper', 'myFlowsApi'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailOutputController', FlowletDetailOutputController);
