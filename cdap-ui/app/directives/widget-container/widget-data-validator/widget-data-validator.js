angular.module(PKG.name + '.commons')
  .directive('myDataValidator', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '=',
        properties: '=',
        inputschema: '='
      },
      templateUrl: 'widget-container/widget-data-validator/widget-data-validator.html',
      controller: function($scope) {
        $scope.properties.rules = $scope.properties.rules || [];
        if (typeof $scope.properties.rules === 'string') {
          $scope.properties.rules = JSON.parse($scope.properties.rules);
        } 
        $scope.rules = $scope.properties.rules;

        $scope.updateScript = function(rules) {

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

          var fnSignature = 'function transform(input, context) {';
          var inbuiltFn = [isGreaterThan, isLessThan, isBlankOrNull, isEqualTo, isPositive, isNumber];
          inbuiltFn.forEach(function(fn) {
            fnSignature += fn.toString();
          });

          var ifExpression = 'if (';
          var endIfExpression = ') {return {result: false}; }';
          $scope.rules.forEach(function(field) {
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
          $scope.properties.script = fnSignature;
          console.info($scope.properties);
        };

        $scope.$watch(function() {
          return $scope.rules;
        }, $scope.updateScript, true);
      }
    };
  });
