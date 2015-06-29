'use strict';
class FlowsRunDetailStatusController {
  constructor($state, $scope, MyDataSource, myHelpers, FlowDiagramData, $timeout, MyMetricsQueryHelper, myFlowsApi) {
    this.$state = $state;
    this.$scope = $scope;
    this.runsCtrl = $scope.RunsController;
    this.MyDataSource = MyDataSource;
    this.myHelpers = myHelpers;
    this.$timeout = $timeout;
    this.MyMetricsQueryHelper = MyMetricsQueryHelper;
    this.myFlowsApi = myFlowsApi;
    this.data = {};

    FlowDiagramData
      .fetchData($state.params.appId, $state.params.programId)
      .then( data => {
        this.data = data;
        this.pollMetrics();
      });
  }

  pollMetrics() {
    let dataSrc = new this.MyDataSource(this.$scope);
    let nodes = this.data.nodes;
    this.data.instances = {};
    // Requesting Metrics data
    angular.forEach(nodes, node => {
      if (node.type !== 'STREAM' && !this.runsCtrl.runs.length) {
        return;
      }
      dataSrc.poll({
        _cdapPath: (node.type === 'STREAM' ? this.generateStreamMetricsPath(node.name): this.generateFlowletMetricsPath(node.name)),
        method: 'POST',
        interval: 2000
      }, data => this.data.metrics[node.name] = this.myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0 );

      // Polling for Flowlet Instance
      if (node.type !== 'STREAM') {
        let params = {
          namespace: this.$state.params.namespace,
          appId: this.$state.params.appId,
          flowId: this.$state.params.programId,
          flowletId: node.name,
          scope: this.$scope
        };
        this.myFlowsApi
            .pollFlowletInstance(params)
            .$promise
            .then(res => this.data.instances[node.name] = res.instances);
      }

    });
  }
  generateStreamMetricsPath (streamName) {
    let streamTags = {
      namespace: this.$state.params.namespace,
      stream: streamName
    };
    let tagsToParams = this.MyMetricsQueryHelper.tagsToParams(streamTags);
    return `/metrics/query?metric=system.collect.events&aggregate=true&${tagsToParams}`;
  }

  generateFlowletMetricsPath (flowletName) {
    let flowletTags = {
      namespace: this.$state.params.namespace,
      app: this.$state.params.appId,
      flow: this.$state.params.programId,
      run: this.runsCtrl.runs.selected.runid,
      flowlet: flowletName
    };
    let tagsToParams = this.MyMetricsQueryHelper.tagsToParams(flowletTags);
    return `/metrics/query?metric=system.process.events.processed&aggregate=true&${tagsToParams}`;
  }
  flowletClick (node) {
    this.runsCtrl.selectTab(this.runsCtrl.tabs[1], node);
  }
}

FlowsRunDetailStatusController.$inject = ['$state', '$scope', 'MyDataSource', 'myHelpers', 'FlowDiagramData', '$timeout', 'MyMetricsQueryHelper', 'myFlowsApi'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailStatusController', FlowsRunDetailStatusController);
