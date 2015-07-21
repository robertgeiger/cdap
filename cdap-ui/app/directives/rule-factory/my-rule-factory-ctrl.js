angular.module(PKG.name + '.commons')
  .controller('MyRuleFactoryCtrl', function($scope) {
    this.inputFields = $scope.inputFields;
    $scope.rules = $scope.rules || [];
    this.fields = $scope.rules;

    function isGreaterThan(input, value) {
      return input > value;
    }

    function isLessThan(input, value) {
      return input < value;
    }

    function isBlankOrNull(input) {
      return input === null || (typeof input === 'string' && !input.length);
    }

    function isEqualTo(input, value) {
      return input === value;
    }

    function isNumber(input) {
      return isNaN(input);
    }

    function isPositive(input) {
      return input > 0;
    }

    this.onFieldClicked = function(field) {
      var isFieldExist = this.fields.filter(function(f) {
        return f.name === field.name;
      });
      if (isFieldExist.length) {
        return;
      }
      this.fields.push({
        name: field.name,
        type: field.type,
        rules: []
      });
    };
    this.removeField = function(fieldObj) {
      var index = this.fields.indexOf(fieldObj);
      if (index !== -1) {
        this.fields.splice(index, 1);
      }
    };

    this.generateScript = function() {
      var fnSignature = 'function transform(input, context) {';
      var inbuiltFn = [isGreaterThan, isLessThan, isBlankOrNull, isEqualTo, isPositive, isNumber];
      inbuiltFn.forEach(function(fn) {
        fnSignature += fn.toString();
      });
      fnSignature += '  ';
      var ifExpression = 'if (';
      var endIfExpression = ') {return {result: false}; }';
      this.fields.forEach(function(field) {
        fnSignature += ifExpression;
        field.rules.forEach(function(rule, index, rules) {
          fnSignature += '!' + rule.name + '(input.' + field.name;
          rule.fields.forEach(function(arg) {
            fnSignature += ', ' + arg;
          });
          fnSignature += ')';
          if (index !== rules.length -1) {
            fnSignature += ' && ';
          }
        });
        fnSignature += endIfExpression;
      });
      fnSignature += 'return {result: true};}';
      console.info(fnSignature);
    };
  });
