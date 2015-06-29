'use strict';
class FlowsFlowletDetailController {
  constructor($state, $scope, myHelpers, myFlowsApi) {
    this.activeTab = 0;
    this.myFlowsApi = myFlowsApi;
    let flowletid = $scope.FlowletsController.activeFlowlet.name;

    this.params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    this.myFlowsApi.get(this.params)
      .$promise
      .then(res => this.description = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'description'));

    this.params.flowletId = flowletid;

    this.myFlowsApi.getFlowletInstance(this.params)
      .$promise
      .then(res => {
        this.provisionedInstances = res.instances;
        this.instance = res.instances;
      });

    this.myFlowsApi.pollFlowletInstance(this.params)
      .$promise
      .then(res => this.provisionedInstances = res.instances);
  }
  setInstance() {
    this.myFlowsApi.setFlowletInstance(this.params, { 'instances': this.instance });
  }
}
FlowsFlowletDetailController.$inject = ['$state', '$scope', 'myHelpers', 'myFlowsApi'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetailController', FlowsFlowletDetailController);
