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
        $scope.rules = $scope.properties.rules;

        $scope.updateScript = function(rules) {
          var fnSignature = 'function transform(input, context) {';

          var ifExpression = 'if (';
          var endIfExpression = ') {return {result: false}; }';
          $scope.rules.forEach(function(field) {
            fnSignature += ifExpression;
            field.rules.forEach(function(rule, index, rules) {
              fnSignature += '!context.' + rule.name + '(input.' + field.name;
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
