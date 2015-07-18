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

package co.cask.cdap.template.etl.transform;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import com.google.common.base.Preconditions;

/**
 * Context for {@link ScriptTransform}.
 */
public class ScriptTransformContext {

  private final DatasetContext datasetContext;

  public ScriptTransformContext(DatasetContext datasetContext) {
    this.datasetContext = datasetContext;
  }

  public Object lookup(String datasetName, String key) {
    Dataset dataset = datasetContext.getDataset(datasetName);
    Preconditions.checkArgument(dataset instanceof ObjectMappedTable, "lookup dataset must be a ObjectMappedTable");
    ObjectMappedTable table = (ObjectMappedTable) dataset;
    return table.read(key);
  }

}
