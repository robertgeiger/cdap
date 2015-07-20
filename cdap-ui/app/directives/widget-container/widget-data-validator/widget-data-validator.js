angular.module(PKG.name + '.commons')
  .directive('myDataValidator', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '=',
        plugins: '='
      },
      templateUrl: 'widget-container/widget-data-validator/widget-data-validator.html',
      controller: function($scope) {
        console.info('From validator: ', $scope.inputSchema);
        debugger;
      }
    };
  });
