'use strict';
class FlowsRunDetailController {
  constructor($scope, $state, $filter) {
    let filterFilter = $filter('filter'),
        match;
    match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    // If there is no match then there is something wrong with the runid in the URL.
    $scope.RunsController.runs.selected.runid = match[0].runid;
    $scope.$on('$destroy', () => $scope.RunsController.runs.selected.runid = null);
  }
}
FlowsRunDetailController.$inject = ['$scope', '$state', '$filter'];

angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailController', FlowsRunDetailController);
