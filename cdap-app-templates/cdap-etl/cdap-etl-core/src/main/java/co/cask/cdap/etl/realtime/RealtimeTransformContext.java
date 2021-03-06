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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.common.PluginID;
import co.cask.cdap.etl.common.ScopedPluginContext;
import co.cask.cdap.etl.common.StageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for the Transform Stage.
 */
public class RealtimeTransformContext extends ScopedPluginContext implements TransformContext {
  private static final Logger LOG = LoggerFactory.getLogger(RealtimeTransformContext.class);
  private final WorkerContext context;
  private final Metrics metrics;

  public RealtimeTransformContext(WorkerContext context, Metrics metrics, String stageId) {
    super(stageId);
    this.context = context;
    this.metrics = metrics;
  }

  @Override
  public Metrics getMetrics() {
    return new StageMetrics(metrics, PluginID.from(stageId));
  }

  @Override
  public int getStageId() {
    return PluginID.from(stageId).getStage();
  }

  @Override
  protected <T> T newScopedPluginInstance(String scopedPluginId) throws InstantiationException {
    return context.newPluginInstance(scopedPluginId);
  }

  @Override
  protected <T> Class<T> loadScopedPluginClass(String scopedPluginId) {
    return context.loadPluginClass(scopedPluginId);
  }

  @Override
  public PluginProperties getPluginProperties() {
    return context.getPluginProperties(stageId);
  }

  @Override
  public PluginProperties getScopedPluginProperties(String scopedPluginId) {
    return context.getPluginProperties(scopedPluginId);
  }
}
