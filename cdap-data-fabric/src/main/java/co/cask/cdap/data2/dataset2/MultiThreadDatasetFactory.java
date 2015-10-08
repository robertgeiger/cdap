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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Implementation of {@link DynamicDatasetFactory} that performs all operations on a per-thread basis.
 * That is, every thread is guaranteed to receive its own distinct copy of every dataset; every thread
 * has its own transaction context, etc.
 */
public class MultiThreadDatasetFactory extends DynamicDatasetFactory {

  // maintains a single threaded factory for each thread.
  private final LoadingCache<Long, SingleThreadDatasetFactory> perThreadMap;

  /**
   * See {@link DynamicDatasetFactory).
   */
  public MultiThreadDatasetFactory(final TransactionSystemClient txClient,
                                   final DatasetFramework datasetFramework,
                                   final ClassLoader classLoader,
                                   final Id.Namespace namespace,
                                   final List<? extends Id> owners,
                                   final Map<String, String> runtimeArguments,
                                   final MetricsContext metricsContext,
                                   @Nullable final Map<String, Map<String, String>> staticDatasets) {
    super(txClient, datasetFramework, classLoader, namespace, owners, runtimeArguments, metricsContext, staticDatasets);
    this.perThreadMap = CacheBuilder.newBuilder()
      .softValues()
      .removalListener(new RemovalListener<Long, DynamicDatasetFactory>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<Long, DynamicDatasetFactory> notification) {
          DynamicDatasetFactory factory = notification.getValue();
          if (factory != null) {
            factory.close();
          }
        }
      })
      .build(
        new CacheLoader<Long, SingleThreadDatasetFactory>() {
          @Override
          @ParametersAreNonnullByDefault
          public SingleThreadDatasetFactory load(Long threadId) throws Exception {
            return new SingleThreadDatasetFactory(txClient, datasetFramework, classLoader, namespace,
                                                  owners, runtimeArguments, metricsContext, staticDatasets);
          }
        });
  }

  public void close() {
    perThreadMap.invalidateAll();
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(DatasetCacheKey key)
    throws DatasetInstantiationException {
    return entryForCurrentThread().getDataset(key);
  }

  public TransactionContext newTransactionContext() {
    return entryForCurrentThread().newTransactionContext();
  }

  public Iterable<TransactionAware> getInProgressTransactionAwares() {
    return entryForCurrentThread().getInProgressTransactionAwares();
  }

  private DynamicDatasetFactory entryForCurrentThread() {
    try {
      return perThreadMap.get(Thread.currentThread().getId());
    } catch (ExecutionException e) {
      // this should never happen because all we do in the cache loader is crete a new entry.
      throw Throwables.propagate(e);
    }
  }

}

