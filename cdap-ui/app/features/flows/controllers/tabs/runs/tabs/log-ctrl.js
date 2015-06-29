'use strict';
class FlowsRunDetailLogController {
  constructor($scope, $state, myFlowsApi) {
    this.logs = [];
    this.runsCtrl = $scope.RunsController;
    if (!this.runsCtrl.runs.length) {
      return;
    }

    let params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      runId: this.runsCtrl.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    myFlowsApi.logs(params)
      .$promise
      .then(res => this.logs = res);
  }
}
FlowsRunDetailLogController.$inject = ['$scope', '$state', 'myFlowsApi'];

angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailLogController', FlowsRunDetailLogController);
