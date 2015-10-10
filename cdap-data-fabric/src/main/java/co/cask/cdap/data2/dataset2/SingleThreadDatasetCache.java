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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Implementation of {@link DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}. Datasets acquired from this context are distinct from any
 * Datasets instantiated outside this class.
 */
public class SingleThreadDatasetCache extends DynamicDatasetCache {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SingleThreadDatasetCache.class);

  private final LoadingCache<DatasetCacheKey, Dataset> datasetCache;
  private final CacheLoader<DatasetCacheKey, Dataset> datasetLoader;
  private final Set<DatasetCacheKey> txnInProgressDatasets = new HashSet<>();
  private TransactionContext txContext = null;

  /**
   * See {@link DynamicDatasetCache}.
   */
  public SingleThreadDatasetCache(final SystemDatasetInstantiator instantiator,
                                  final TransactionSystemClient txClient,
                                  final Id.Namespace namespace,
                                  @Nullable final List<? extends Id> owners,
                                  Map<String, String> runtimeArguments,
                                  @Nullable final MetricsContext metricsContext,
                                  @Nullable Map<String, Map<String, String>> staticDatasets) {
    super(instantiator, txClient, namespace, owners, runtimeArguments, metricsContext, staticDatasets);
    this.datasetLoader = new CacheLoader<DatasetCacheKey, Dataset>() {
      @Override
      @ParametersAreNonnullByDefault
      public Dataset load(DatasetCacheKey key) throws Exception {
        Dataset dataset = instantiator.getDataset(
          Id.DatasetInstance.from(namespace, key.getName()), key.getArguments());
        if (dataset instanceof MeteredDataset && metricsContext != null) {
          ((MeteredDataset) dataset).setMetricsCollector(
            metricsContext.childContext(Constants.Metrics.Tag.DATASET, key.getName()));
        }
        return dataset;
      }
    };
    this.datasetCache = CacheBuilder.newBuilder().removalListener(
      new RemovalListener<DatasetCacheKey, Dataset>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<DatasetCacheKey, Dataset> notification) {
          if (notification.getValue() != null) {
            try {
              notification.getValue().close();
            } catch (Throwable e) {
              LOG.warn("Error closing dataset '{}'", notification.getKey());
            }
          }
        }
      })
      .build(datasetLoader);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(DatasetCacheKey key, boolean bypass)
    throws DatasetInstantiationException {

    Dataset dataset;
    try {
      if (bypass) {
        dataset = datasetLoader.load(key);
      } else {
        try {
          dataset = datasetCache.get(key);
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }
    } catch (Throwable t) {
      throw new DatasetInstantiationException(
        String.format("Could not instantiate dataset '%s'", key.getName()), t);
    }
    // make sure the dataset exists and is of the right type
    if (dataset == null) {
      throw new DatasetInstantiationException(String.format("Dataset '%s' does not exist", key.getName()));
    }
    try {
      @SuppressWarnings("unchecked")
      T typedDataset = (T) dataset;

      // any transaction aware that is new to the current tx is added to the current tx context (if there is one).
      if (!bypass && dataset instanceof TransactionAware && txnInProgressDatasets.add(key) && txContext != null) {
        txContext.addTransactionAware((TransactionAware) dataset);
      }
      return typedDataset;

    } catch (Throwable t) { // must be ClassCastException
      throw new DatasetInstantiationException(
        String.format("Could not cast dataset '%s' to requested type. Actual type is %s.",
                      key.getName(), dataset.getClass().getName()), t);
    }
  }

  /**
   * Return a new transaction context for the current thread. Any transaction-aware datasets obtained via
   * (@link #getDataset()) from the same thread will be added to this transaction context and thus participate
   * in its transaction. These datasets can also be retrieved using {@link #getTransactionAwares()}.
   *
   * @return a new transactiopn context
   */
  @Override
  public TransactionContext newTransactionContext() {
    txContext = new TransactionContext(txClient);
    // make sure all static transaction-aware datasets participate in the transaction
    txnInProgressDatasets.clear();
    for (DatasetCacheKey key : staticDatasetKeys) {
      getDataset(key, false);
    }
    for (TransactionAware txAware : extraTxAwares) {
      txContext.addTransactionAware(txAware);
    }
    return txContext;
  }

  @Override
  public Iterable<TransactionAware> getTransactionAwares() {
    return Iterables.concat(
      extraTxAwares, Iterables.transform(txnInProgressDatasets,
                                         new Function<DatasetCacheKey, TransactionAware>() {
                                           @Nullable
                                           @Override
                                           public TransactionAware apply(DatasetCacheKey key) {
                                             try {
                                               return (TransactionAware) datasetCache.get(key);
                                             } catch (ExecutionException e) {
                                               throw new IllegalStateException(
                                                 "Unexpected exception from dataset cache for dataset key " +
                                                   key + ", which is in current in-progress transaction and " +
                                                   "should thus already be in the cache", e);
                                             }
                                           }
                                         }));
  }

  @Override
  public void invalidate() {
    txnInProgressDatasets.clear();
    try {
      datasetCache.invalidateAll();
    } catch (Throwable t) {
      LOG.error("Error invalidating dataset cache", t);
    }
    try {
      datasetCache.cleanUp();
    } catch (Throwable t) {
      LOG.error("Error cleaning up dataset cache", t);
    }
  }

  @Override
  public void close() {
    super.close();
    invalidate();
  }
}

