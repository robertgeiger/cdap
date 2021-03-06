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

angular.module(PKG.name + '.commons')
  .directive('loadingIcon', function(myLoadingService, $bootstrapModal, $timeout, EventPipe, $state, $alert) {
    return {
      restrict: 'EA',
      scope: true,
      template: '<div></div>',
      controller: function($scope) {
        var modalObj = {
          templateUrl: 'app-level-loading-icon/loading.html',
          backdrop: 'static',
          keyboard: true,
          scope: $scope,
          windowClass: 'custom-loading-modal'
        }, modal, isBackendDown = false;

        EventPipe.on('backendDown', function(message, subtitle) {
          if (!isBackendDown) {
            if (modal) {
              modal.close();
            }
            isBackendDown = true;
            if (!message) {
              $scope.message = 'Service(s) are offline';
            } else {
              $scope.message = message;
              $scope.subtitle = subtitle;
            }
            modal = $bootstrapModal.open(modalObj);
            modal.result.finally(function() {
              $state.go('overview', {}, {reload: true});
            });
          }
        }.bind($scope));

        EventPipe.on('backendUp', function(message) {
          if (isBackendDown) {
            modal.close();
            modal = null;
            isBackendDown = false;

            $alert({
              type: 'success',
              content: message ? message : 'Services are online'
            });
          }
        }.bind($scope));

        EventPipe.on('hideLoadingIcon', function() {
          // Just making it smooth instead of being too 'speedy'
          $timeout(function() {
            if (!isBackendDown) {
              if (modal && !modal.$state) {
                modal.close();
              }
              modal = null;
            }
          }, 2000);
        });

        // Should use this hide when we are just loading a state
        EventPipe.on('hideLoadingIcon.immediate', function() {
          if (modal){
            // This is needed if the loading icon is shown and closed even before opened.
            // EventPipe will execute the listener immediately when the event is emitted,
            // however $alert which internally used $modal opens up only during next tick.
            // If the modal is opened and is closed at some point later (normal usecase),
            // the 'opened' promise is still resolved and the alert is closed.
            modal.opened.then(function() {
              modal.close();
              modal = null;
            });
          }
        });

        EventPipe.on('showLoadingIcon', function(message) {
          if(!modal && !isBackendDown) {
            $scope.message = message || '';
            modal = $bootstrapModal.open(modalObj);
          }
        }.bind($scope));
      }
    };
  });
