angular.module(PKG.name + '.feature.adapters')
  .controller('HydratorDetailController', function($scope, rAdapterDetail) {
    $scope.template = rAdapterDetail.template;
    $scope.isScheduled = false;
    if (rAdapterDetail.schedule) {
      $scope.isScheduled = true;
    }
  });
