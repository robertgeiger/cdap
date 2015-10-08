/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Objects;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}. Datasets acquired from this context are
 * distinct from any Datasets instantiated outside this class.
 */
public abstract class DynamicDatasetFactory implements DatasetContext {

  protected final TransactionSystemClient txClient;
  protected final DatasetFramework datasetFramework;
  protected final ClassLoader classLoader;
  protected final Id.Namespace namespace;
  protected final List<? extends Id> owners;
  protected final Map<String, String> runtimeArguments;
  protected final MetricsContext metricsContext;
  protected final Set<DatasetCacheKey> staticDatasetKeys;

  /**
   * Create a dynamic dataset factory.
   *
   * @param txClient the transaction system client to use for new transaction contexts
   * @param datasetFramework the dataset framework for creating dataset instances
   * @param classLoader the classloader to use when creating dataset instances
   * @param namespace the {@link Id.Namespace} in which all datasets are instantiated
   * @param owners the {@link Id}s which own this context
   * @param runtimeArguments all runtime arguments that are available to datasets in the context. Runtime arguments
   *                         are expected to be scoped so that arguments for one dataset do not override arguments
   * @param metricsContext if non-null, this context is used as the context for dataset metrics,
   *                       with an additional tag for the dataset name.
   * @param staticDatasets  if non-null, a map from dataset name to runtime arguments. These datasets will be
   *                        instantiated immediately, and they will participate in every transaction started
   *                        through {@link #newTransactionContext()}.
   */
  public DynamicDatasetFactory(TransactionSystemClient txClient,
                               DatasetFramework datasetFramework,
                               ClassLoader classLoader,
                               Id.Namespace namespace,
                               @Nullable List<? extends Id> owners,
                               Map<String, String> runtimeArguments,
                               @Nullable MetricsContext metricsContext,
                               @Nullable Map<String, Map<String, String>> staticDatasets) {
    this.txClient = txClient;
    this.datasetFramework = datasetFramework;
    this.classLoader = classLoader;
    this.namespace = namespace;
    this.owners = owners;
    this.runtimeArguments = runtimeArguments;
    this.metricsContext = metricsContext;
    this.staticDatasetKeys = new HashSet<>();
    if (staticDatasets != null) {
      for (Map.Entry<String, Map<String, String>> entry : staticDatasets.entrySet()) {
        staticDatasetKeys.add(new DatasetCacheKey(entry.getKey(), entry.getValue()));
      }
    }
  }

  @Override
  public final <T extends Dataset> T getDataset(String name)
    throws DatasetInstantiationException {
    return getDataset(name, DatasetDefinition.NO_ARGUMENTS);
  }

  @Override
  public final <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    // apply actual runtime arguments on top of the context's runtime arguments for this dataset
    Map<String, String> dsArguments =
      RuntimeArguments.extractScope(Scope.DATASET, name, runtimeArguments);
    dsArguments.putAll(arguments);
    return getDataset(new DatasetCacheKey(name, dsArguments));
  }

  /**
   * To be implemented by subclasses
   */
  protected abstract <T extends Dataset> T getDataset(DatasetCacheKey key)
    throws DatasetInstantiationException;

  /**
   * Return a new transaction context. Any transaction-aware datasets obtained via
   * (@link #getDataset()) will be added to this transaction context and thus participate
   * in its transaction. These datasets can also be retrieved using {@link #getInProgressTransactionAwares()}.
   *
   * @return a new transactiopn context
   */
  public abstract TransactionContext newTransactionContext();

  /**
   * @return the transaction-aware datasets that participate in the current transaction.
   */
  public abstract Iterable<TransactionAware> getInProgressTransactionAwares();

  /**
   * Close and dismiss all datasets that were obtained through this factory.
   */
  public abstract void close();

  /**
   * A key used by implementations of {@link DynamicDatasetFactory} to cache Datasets. Includes the dataset name
   * and it's arguments.
   */
  public static final class DatasetCacheKey {
    private final String name;
    private final Map<String, String> arguments;

    public DatasetCacheKey(String name, Map<String, String> arguments) {
      this.name = name;
      this.arguments = arguments == null ? DatasetDefinition.NO_ARGUMENTS : arguments;
    }

    public String getName() {
      return name;
    }

    public Map<String, String> getArguments() {
      return arguments;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DatasetCacheKey that = (DatasetCacheKey) o;

      return Objects.equal(this.name, that.name) &&
        Objects.equal(this.arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, arguments);
    }
  }
}

