/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * myNavbar
 */

angular.module(PKG.name+'.commons').directive('myNavbarHydrator',

function myNavbarHydratorDirective (myAuth, MY_CONFIG) {
  return {
    restrict: 'A',
    templateUrl: 'navbar-hydrator/navbar.html',
    link: function (scope) {
      scope.logout = myAuth.logout;
      scope.securityEnabled = MY_CONFIG.securityEnabled;
    },
    controller: function($scope, $state) {
      $scope.highlightTab = $state.current.data && $state.current.data.highlightTab;
      $scope.$on('$stateChangeSuccess', function(event, toState) {
        $scope.highlightTab = toState.data && toState.data.highlightTab;
      });
    }
  };
});
