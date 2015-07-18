angular.module(PKG.name + '.commons')
  .controller('MyRuleCtrl', function($scope) {
    this.rulesMap = {
      isBlankOrNull: {
        numFields: []
      },
      isValidEmail: {
        numFields: []
      },
      isMaxLength: {
        numFields: ['text']
      },
      isGreaterThan: {
        numFields: ['text']
      },
      isLessThan: {
        numFields: ['text']
      },
      isEqualTo: {
        numFields: ['text']
      },
      isNumber: {
        numFields: ['text']
      },
      isPositive: {
        numFields: ['text', 'text', 'text', 'text']
      }
    };
    this.ruleToTypeMap = {
      number: ['isGreaterThan', 'isLessThan', 'isEqualTo', 'isNumber', 'isPositive'],
      string: ['isBlankOrNull', 'isValidEmail', 'isMaxLength']
    }
    this.rule = $scope.rule || {};
    this.rule.name = this.rule.name;
    this.rule.fields = this.rule.fields || [];
    this.type = $scope.type;
  });
