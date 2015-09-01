angular.module(PKG.name+'.commons')
  .controller('myFlowController', function($scope, myHelpers) {
    function update(newVal) {
      // Avoid rendering the graph without nodes and edges.
      if (myHelpers.objectQuery(newVal, 'nodes') && myHelpers.objectQuery(newVal, 'edges')) {
        $scope.render();
      }
    }

    $scope.instanceMap = {};
    $scope.labelMap = {};

    // This is done because of performance reasons.
    // Earlier we used to have scope.$watch('model', function, true); which becomes slow with large set of
    // nodes. So the controller/component that is using this directive need to pass in this flag and update it
    // whenever there is a change in the model. This way the watch becomes smaller.

    // The ideal solution would be to use a service and have this directive register a callback to the service.
    // Once the service updates the data it could call the callbacks by updating them with data. This way there
    // is no watch. This is done in adapters and we should fix this ASAP.
    $scope.$watch('onChangeFlag', function(newVal) {
      if (newVal) {
        update($scope.model);
      }
    });

  });
