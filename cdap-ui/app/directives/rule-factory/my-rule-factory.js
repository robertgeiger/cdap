angular.module(PKG.name + '.commons')
  .directive('myRuleFactory', function() {
    return {
      restrict: 'E',
      scope: {
        inputFields: '=',
        rules: '='
      },
      templateUrl: 'rule-factory/my-rule-factory.html',
      controller: 'MyRuleFactoryCtrl',
      controllerAs: 'MyRuleFactoryCtrl'
    };
  });
