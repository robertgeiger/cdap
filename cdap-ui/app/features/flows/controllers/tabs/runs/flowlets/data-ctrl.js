'use strict';
class FlowletDetailDataController {
  constructor($state, $scope, MyDataSource, myHelpers, MyMetricsQueryHelper, myFlowsApi) {
    this.$state = $state;
    this.datasets = [];
    this.MyDataSource = MyDataSource;
    this.MyMetricsQueryHelper = MyMetricsQueryHelper;

    let dataSrc = new MyDataSource($scope);
    let flowletid = $scope.FlowletsController.activeFlowlet.name;
    let params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi.get(params)
      .$promise
      .then(res => {
        let obj = [];
        let datasets = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'dataSets');
        angular.forEach( datasets, v => obj.push({ name: v }) );
        this.datasets = obj;
        this.pollDatasets();
      });
  }
  pollDatasets() {
    angular.forEach(this.datasets, dataset => {
      let datasetTags = {
        namespace: this.$state.params.namespace,
        dataset: dataset.name,
        app: this.$state.params.appId,
        flow: this.$state.params.programId
      };
      let tagsToParams = this.MyMetricsQueryHelper.tagsToParams(datasetTags);
      let dataSrc = new this.MyDataSource(this.$scope);

      dataSrc
        .poll({
          _cdapPath: `/metrics/query?${tagsToParams}&metric=system.dataset.store.reads`,
          method: 'POST'
        }, res => {
          if (res.series[0]) {
            dataset.reads = res.series[0].data[0].value;
          }
        });

      dataSrc
        .poll({
          _cdapPath: `/metrics/query?${tagsToParams}&metric=system.dataset.store.writes`,
          method: 'POST'
        }, res => {
          if (res.series[0]) {
            dataset.writes = res.series[0].data[0].value;
          }
        });
    });
  }
}
FlowletDetailDataController.$inject = ['$state', '$scope', 'MyDataSource', 'myHelpers', 'MyMetricsQueryHelper', 'myFlowsApi'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailDataController', FlowletDetailDataController);
