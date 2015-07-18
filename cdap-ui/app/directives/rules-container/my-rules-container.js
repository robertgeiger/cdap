angular.module(PKG.name + '.commons')
  .directive('myRulesContainer', function() {
    return {
      restrict: 'E',
      scope: {
        fieldObj: '='
      },
      templateUrl: 'rules-container/my-rules-container.html',
      controller: 'MyRulesContainerCtrl',
      controllerAs: 'MyRulesContainerCtrl'
    };
  });
