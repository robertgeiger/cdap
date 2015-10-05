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
package co.cask.cdap.data2.datafabric.store;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public class DefaultNamespaceStore implements NamespaceStore {

  public static final Id.DatasetInstance APP_META_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, Constants.AppMetaStore.TABLE);

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceStore.class);

  private final TransactionExecutorFactory txExecutorFactory;
  private final Supplier<Table> appMetaTable;
  private final DatasetFramework framework;
  private final NamespacedLocationFactory namespacedLocationFactory;

  @Inject
  public DefaultNamespaceStore(TransactionExecutorFactory txExecutorFactory, DatasetFramework framework,
                               NamespacedLocationFactory namespacedLocationFactory) {
    this.framework = framework;
    this.txExecutorFactory = txExecutorFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    // TODO: make expiration configurable?
    this.appMetaTable = Suppliers.memoizeWithExpiration(
      new Supplier<Table>() {
        @Override
        public Table get() {
          try {
            return DatasetsUtil.getOrCreateDataset(
              getDatasetFramework(), APP_META_INSTANCE_ID, "table",
              DatasetProperties.EMPTY,
              DatasetDefinition.NO_ARGUMENTS, null);
          } catch (DatasetManagementException | IOException e) {
            LOG.error("Error getting appMetaTable", e);
            // TODO: handle this better
            return null;
          }
        }
      }, 10, TimeUnit.SECONDS
    );
  }

  protected DatasetFramework getDatasetFramework() {
    return framework;
  }

  private <O> O executeUnchecked(final TransactionExecutor.Function<NamespaceMetadataStore, O> func) {
    TransactionAware txAware = (TransactionAware) Preconditions.checkNotNull(
      appMetaTable.get(), "Couldn't get appMetaTable");
    return txExecutorFactory.createExecutor(Collections.singleton(txAware))
      .executeUnchecked(new TransactionExecutor.Function<Table, O>() {
        @Override
        public O apply(Table table) throws Exception {
          NamespaceMetadataStore mds = new NamespaceMetadataStore(table);
          return func.apply(mds);
        }
      }, appMetaTable.get());
  }

  @Override
  @Nullable
  public NamespaceMeta createNamespace(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    return executeUnchecked(new TransactionExecutor.Function<NamespaceMetadataStore, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMetadataStore mds) throws Exception {
        Id.Namespace namespaceId = Id.Namespace.from(metadata.getName());
        NamespaceMeta existing = mds.get(namespaceId);
        if (existing != null) {
          return existing;
        }
        mds.create(metadata);
        return null;
      }
    });
  }

  @Override
  public void updateNamespace(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    executeUnchecked(new TransactionExecutor.Function<NamespaceMetadataStore, Void>() {
      @Override
      public Void apply(NamespaceMetadataStore input) throws Exception {
        NamespaceMeta existing = input.get(Id.Namespace.from(metadata.getName()));
        if (existing != null) {
          input.create(metadata);
        }
        return null;
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta getNamespace(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return executeUnchecked(new TransactionExecutor.Function<NamespaceMetadataStore, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMetadataStore input) throws Exception {
        return input.get(id);
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta deleteNamespace(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return executeUnchecked(new TransactionExecutor.Function<NamespaceMetadataStore, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMetadataStore input) throws Exception {
        NamespaceMeta existing = input.get(id);
        if (existing != null) {
          input.delete(id);
        }
        return existing;
      }
    });
  }

  @Override
  public List<NamespaceMeta> listNamespaces() {
    return executeUnchecked(new TransactionExecutor.Function<NamespaceMetadataStore, List<NamespaceMeta>>() {
      @Override
      public List<NamespaceMeta> apply(NamespaceMetadataStore input) throws Exception {
        return input.list();
      }
    });
  }
}
