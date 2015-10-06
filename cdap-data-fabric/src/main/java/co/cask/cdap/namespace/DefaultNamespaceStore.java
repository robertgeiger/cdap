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

package co.cask.cdap.namespace;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link NamespaceStore} that ultimately places data into MetaDataTable.
 */
public class DefaultNamespaceStore implements NamespaceStore {

  // TODO: currently shares a table with DefaultStore - may want to separate later
  private static final Id.DatasetInstance APP_META_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, Constants.AppMetaStore.TABLE);

  private final DatasetFramework dsFramework;

  private Transactional<NamespaceMds, NamespaceMetadataStore> txnl;

  @Inject
  public DefaultNamespaceStore(TransactionExecutorFactory txExecutorFactory, DatasetFramework framework) {
    this.dsFramework = framework;

    txnl = Transactional.of(txExecutorFactory, new Supplier<NamespaceMds>() {
      @Override
      public NamespaceMds get() {
        try {
          Table mdsTable = DatasetsUtil.getOrCreateDataset(
            dsFramework, APP_META_INSTANCE_ID, "table",
            DatasetProperties.EMPTY,
            DatasetDefinition.NO_ARGUMENTS, null);
          return new NamespaceMds(mdsTable);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta create(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMds input) throws Exception {
        Id.Namespace namespaceId = Id.Namespace.from(metadata.getName());
        NamespaceMeta existing = input.namespaces.getNamespace(namespaceId);
        if (existing != null) {
          return existing;
        }
        input.namespaces.createNamespace(metadata);
        return null;
      }
    });
  }

  @Override
  public void update(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Void>() {
      @Override
      public Void apply(NamespaceMds input) throws Exception {
        NamespaceMeta existing = input.namespaces.getNamespace(Id.Namespace.from(metadata.getName()));
        if (existing != null) {
          input.namespaces.createNamespace(metadata);
        }
        return null;
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta get(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMds input) throws Exception {
        return input.namespaces.getNamespace(id);
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta delete(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMds input) throws Exception {
        NamespaceMeta existing = input.namespaces.getNamespace(id);
        if (existing != null) {
          input.namespaces.deleteNamespace(id);
        }
        return existing;
      }
    });
  }

  @Override
  public List<NamespaceMeta> list() {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, List<NamespaceMeta>>() {
      @Override
      public List<NamespaceMeta> apply(NamespaceMds input) throws Exception {
        return input.namespaces.listNamespaces();
      }
    });
  }

  @Override
  public boolean exists(Id.Namespace id) {
    return get(id) != null;
  }

  private static final class NamespaceMds implements Iterable<NamespaceMetadataStore> {
    private final NamespaceMetadataStore namespaces;

    private NamespaceMds(Table mdsTable) {
      this.namespaces = new NamespaceMetadataStore(mdsTable);
    }

    @Override
    public Iterator<NamespaceMetadataStore> iterator() {
      return Iterators.singletonIterator(namespaces);
    }
  }
}
