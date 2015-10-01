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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Implementation of {@link UsageRegistry} to be used by system services. Allows for operations without remote calls.
 */
public class SystemUsageRegistry extends UsageRegistry {

  private final DatasetFramework datasetFramework;

  @Inject
  public SystemUsageRegistry(TransactionExecutorFactory txExecutorFactory,
                             DatasetFramework datasetFramework) {
    super(txExecutorFactory);
    this.datasetFramework = datasetFramework;
  }

  @Override
  public UsageDataset getUsageDataset() throws IOException, DatasetManagementException {
    Object usageDataset = DatasetsUtil.getOrCreateDataset(datasetFramework, USAGE_INSTANCE_ID,
                                                          UsageDataset.class.getSimpleName(),
                                                          DatasetProperties.EMPTY, null, null);
    // Backward compatible check for version <= 3.0.0
    if (usageDataset instanceof UsageDataset) {
      return (UsageDataset) usageDataset;
    }
    return new UsageDataset((Table) usageDataset);
  }
}
