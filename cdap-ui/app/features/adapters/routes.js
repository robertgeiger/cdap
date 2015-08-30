angular.module(PKG.name + '.feature.adapters')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('hydrator', {
        url: '/hydrator',
        abstract: true,
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

        .state('hydrator.drafts', {
          url: '/drafts',
          templateUrl: '/assets/features/adapters/templates/drafts.html',
          controller: 'HydratorDraftsController',
          ncyBreadcrumb: {
            label: 'All Drafts',
            parent: 'overview'
          }
        })

        .state('hydrator.lists', {
          url: '',
          templateUrl: '/assets/features/adapters/templates/list.html',
          controller: 'HydratorListController',
          controlelrAs: 'ListController'
        })

        // Adater create controller + template rename will happen parallel in adapter create revamp
        .state('hydrator.create', {
          url: '/create?name&type',
          params: {
            data: null
          },
          resolve: {
            rConfig: function($stateParams, mySettings, $q) {
              var defer = $q.defer();
              if ($stateParams.name) {
                mySettings.get('adapterDrafts')
                  .then(function(res) {
                    var draft = res[$stateParams.name];
                    if (angular.isObject(draft)) {
                      draft.name = $stateParams.name;
                      defer.resolve(draft);
                    } else {
                      defer.resolve(false);
                    }
                  });
              } else if ($stateParams.data){
                defer.resolve($stateParams.data);
              } else {
                defer.resolve(false);
              }
              return defer.promise;
            }
          },
          controller: '_AdapterCreateController as AdapterCreateController',
          templateUrl: '/assets/features/adapters/templates/create.html',
          ncyBreadcrumb: {
            skip: true
          }
        })

        .state('hydrator.detail', {
          url: '/:hydratorId',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          resolve : {
            rRuns: function($stateParams, $q, myAdapterApi) {
              var defer = $q.defer();
              // Using _cdapPath here as $state.params is not updated with
              // runid param when the request goes out
              // (timing issue with re-direct from login state).
              var params = {
                namespace: $stateParams.namespace,
                adapter: $stateParams.hydratorId
              };

              myAdapterApi.runs(params)
                .$promise
                .then(function(res) {
                  defer.resolve(res);
                });
              return defer.promise;
            },
            rAdapterDetail: function($stateParams, $q, myAdapterApi) {
              var params = {
                namespace: $stateParams.namespace,
                adapter: $stateParams.hydratorId
              };

              return myAdapterApi.get(params).$promise;
            }
          },
          ncyBreadcrumb: {
            parent: 'apps.list',
            label: '{{$state.params.hydratorId}}'
          },
          templateUrl: '/assets/features/adapters/templates/detail.html',
          controller: 'HydratorDetailController'
        })
          .state('hydrator.detail.runs',{
            url: '/runs',
            templateUrl: '/assets/features/adapters/templates/tabs/runs.html',
            controller: 'HydratorRunsController',
            ncyBreadcrumb: {
              parent: 'apps.list',
              label: '{{$state.params.adapterId}}'
            }
          })
            .state('hydrator.detail.runs.run', {
              url: '/:runid',
              templateUrl: '/assets/features/adapters/templates/tabs/runs/run-detail.html',
              ncyBreadcrumb: {
                label: '{{$state.params.runid}}'
              }
            })

        .state('hydrator.detail.datasets', {
          url: '/datasets',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: 'data-list/data-list.html',
          controller: 'HydratorDatasetsController',
          ncyBreadcrumb: {
            label: 'Datasets',
            parent: 'Hydrator.detail.runs'
          }
        })
        .state('hydrator.detail.history', {
          url: '/history',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: '/assets/features/adapters/templates/tabs/history.html',
          controller: 'HydratorRunsController',
          ncyBreadcrumb: {
            label: 'History',
            parent: 'Hydrator.detail.runs'
          }
        })
        .state('hydrator.detail.schedule', {
          url: '/schedule',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: '/assets/features/adapters/templates/tabs/schedule.html',
          controller: 'ScheduleController',
          controllerAs: 'ScheduleController',
          ncyBreadcrumb: {
            label: 'Schedule',
            parent: 'Hydrator.detail.runs'
          }
        });
  });
