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

package co.cask.cdap.app.runtime.scheduler;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.store.NamespaceStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;

import javax.annotation.Nullable;


/**
 * Helper class to resolve scheduler queue name.
 */
public class SchedulerQueueResolver {
  private final String defaultQueue;
  private final NamespaceStore nsStore;

  /**
   * Construct SchedulerQueueResolver with CConfiguration and Store.
   */
  public SchedulerQueueResolver(CConfiguration cConf, NamespaceStore nsStore) {
    this.defaultQueue = cConf.get(Constants.AppFabric.APP_SCHEDULER_QUEUE);
    this.nsStore = nsStore;
  }

  /**
   * @return default Queue that comes from CConfiguration.
   */
  public String getDefaultQueue() {
    return defaultQueue;
  }

  /**
   * Get queue at namespace level if it is empty returns the default queue.
   *
   * @param namespaceId NamespaceId
   * @return schedule queue at namespace level or default queue.
   */
  @Nullable
  public String getQueue(Id.Namespace namespaceId) {
    NamespaceMeta meta = nsStore.getNamespace(namespaceId);
    if (meta != null) {
      NamespaceConfig config = meta.getConfig();
      return config.getSchedulerQueueName() != null ? config.getSchedulerQueueName() : getDefaultQueue();
    } else {
      return getDefaultQueue();
    }
  }
}
