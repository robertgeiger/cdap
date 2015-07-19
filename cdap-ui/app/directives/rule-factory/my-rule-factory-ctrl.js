angular.module(PKG.name + '.commons')
  .controller('MyRuleFactoryCtrl', function($scope) {
    this.inputFields = $scope.inputFields;
    this.fields = [];
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
      var isBlankOrNull = function isBlankOrNull(input) {
        return input === null || (typeof input === 'string' && input.length === 0);
      };
      var isGreaterThan = function isGreaterThan(input, value) {
        return input > value;
      };
      var isLessThan = function isLessThan(input, value) {
        return input < value;
      };
      var isEqualTo = function isEqualTo(input, value) {
        return input === value;
      };
      var isNumber = function isNumber(input) {
        return isNaN(input);
      };
      var isMaxLength = function isMaxLength(input, length) {
        return input.length > length;
      };
      var isPositive = function isPositive(input) {
        return input > 0;
      };
      var isValidEmail = function isValidEmail(input) {
        var re = /\S+@\S+\.\S+/;
        return re.test(input);
      };

      var validationFunctions = [
        isBlankOrNull, isGreaterThan, isLessThan, isEqualTo, isNumber,
        isMaxLength, isPositive, isValidEmail
      ];
      validationFunctions.forEach(function(fn) {
        fnSignature += fn.toString();
      });
      var ifExpression = 'if (';
      var endIfExpression = ') {return {result: false}; }';
      this.fields.forEach(function(field) {
        fnSignature += ifExpression;
        field.rules.forEach(function(rule) {
          fnSignature += '!' + rule.name + '(input.' + field.name;
          rule.fields.forEach(function(arg) {
            fnSignature += ', ' + arg;
          });
          fnSignature += ')';
          fnSignature += endIfExpression;
        });
      });
      fnSignature += 'return {result: true};}';
      console.info(fnSignature);
    };
  });
