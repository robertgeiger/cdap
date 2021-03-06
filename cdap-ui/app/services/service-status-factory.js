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

angular.module(PKG.name + '.services')
  .service('ServiceStatusFactory', function(MyDataSource, $timeout, EventPipe, $state, myAuth) {
    this.systemStatus = 'green';

    // Apart from invalid token there should be no scenario
    // when we should stop this poll.
    var dataSrc = new MyDataSource();
    dataSrc.poll({
      _cdapPath: '/system/services/status',
      interval: 10000
    },
    function success(res) {
      var serviceStatuses = Object.keys(res).map(function(value) {
        return res[value];
      });
      if (serviceStatuses.indexOf('NOTOK') > -1) {
        this.systemStatus = 'yellow';
      }
      if (serviceStatuses.indexOf('OK') === -1) {
        this.systemStatus = 'red';
      }
      if (serviceStatuses.indexOf('NOTOK') === -1) {
        this.systemStatus = 'green';
      }
    }.bind(this),
    function error(err) {
      // Check for invalid token if security is enabled.
      if (angular.isObject(err) && err.auth_uri) {
        $timeout(function() {
          EventPipe.emit('backendUp');
          myAuth.logout();
        });
      } else {
        EventPipe.emit('backendDown');
      }
    }
    );
  });
