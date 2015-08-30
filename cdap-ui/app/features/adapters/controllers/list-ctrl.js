angular.module(PKG.name + '.feature.adapters')
  .controller('HydratorListController', function(myAdapterApi, $state) {
    this.appsList = [];
    var params = {
      namespace: $state.params.namespace
    };
    myAdapterApi
      .list(params)
      .$promise
      .then(
        function success(res) {
          this.appsList = res;
        }.bind(this),
        function error() {

        }
      );
  });
