'use strict';
class FlowletsController {
  constructor ($scope, $state, $filter, FlowDiagramData) {
    this.$filter = $filter;
    this.flowlets = [];
    this.runsCtrl = $scope.RunsController;
    let filterFilter = $filter('filter');

    FlowDiagramData
      .fetchData($state.params.appId, $state.params.programId)
      .then(res => {
        angular.forEach(res.flowlets, v => {
          var name = v.flowletSpec.name;
          v.isOpen = false;
          this.flowlets.push({name: name, isOpen: $state.params.flowletid === name});
        });

        if (!this.runsCtrl.activeFlowlet) {
          this.flowlets[0].isOpen = true;
          this.activeFlowlet = this.flowlets[0];
        } else {
          var match = filterFilter(this.flowlets, {name: this.runsCtrl.activeFlowlet});
          match[0].isOpen = true;
          this.activeFlowlet = match[0];
        }
      });
  }

  selectFlowlet (flowlet) {
    let filterFilter = this.$filter('filter');
    angular.forEach(this.flowlets, f => f.isOpen = false);
    let match = filterFilter(this.flowlets, flowlet);
    match[0].isOpen = true;
    this.activeFlowlet = match[0];
  }
}
FlowletsController.$inject = ['$scope', '$state', '$filter', 'FlowDiagramData'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowletsController', FlowletsController);
