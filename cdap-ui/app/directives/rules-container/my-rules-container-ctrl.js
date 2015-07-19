angular.module(PKG.name + '.commons')
  .controller('MyRulesContainerCtrl', function($scope) {
    this.fieldObj = $scope.fieldObj;
    this.fieldObj.rules.push({
      name: 'isGreaterThan'
    });
    this.addRule = function() {
      this.fieldObj.rules.push({
        name: 'isGreaterThan'
      });
    };
  });
