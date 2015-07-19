angular.module(PKG.name + '.commons')
  .controller('MyRuleFactoryCtrl', function($scope) {
    this.inputFields = $scope.inputFields;
    this.fields = [];
    this.onFieldClicked = function(field) {
      this.fields.push({
        name: field.name,
        type: field.type,
        rules: []
      });
    };
    this.generateScript = function() {
      
    };
  });
