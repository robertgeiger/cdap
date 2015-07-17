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

package co.cask.cdap.template.etl.realtime;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.template.etl.api.TransformContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Context for the Transform Stage.
 */
public class RealtimeTransformContext implements TransformContext {
  private final WorkerContext context;
  private final Metrics metrics;

  protected final String pluginPrefix;
  private final AtomicReference<DatasetContext> datasetContextRef;

  public RealtimeTransformContext(WorkerContext context, Metrics metrics, String pluginPrefix) {
    this.context = context;
    this.metrics = metrics;
    this.pluginPrefix = pluginPrefix;
    this.datasetContextRef = new AtomicReference<>();
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public PluginProperties getPluginProperties() {
    return context.getPluginProperties(pluginPrefix);
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    if (datasetContextRef.get() == null) {
      throw new IllegalStateException("Transaction is not active");
    }
    return datasetContextRef.get().getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    if (datasetContextRef.get() == null) {
      throw new IllegalStateException("Transaction is not active");
    }
    return datasetContextRef.get().getDataset(name, arguments);
  }

  void resetDatasetContext(DatasetContext datasetContext) {
    datasetContextRef.set(datasetContext);
  }
}
