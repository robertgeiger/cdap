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

package co.cask.cdap.explore.store;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.view.ViewProperties;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link ExploreViewStore}.
 */
public final class MDSExploreViewStore implements ExploreViewStore {

  private static final Logger LOG = LoggerFactory.getLogger(MDSExploreViewStore.class);

  private static final Id.DatasetInstance EXPLORE_VIEW_STORE_DATASET_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, Constants.Explore.VIEW_STORE_TABLE);
  private static final String TYPE_VIEW = "view";
  private final Transactional<ViewMds, MetadataStoreDataset> txnl;

  @Inject
  public MDSExploreViewStore(final DatasetFramework datasetFramework,
                             TransactionExecutorFactory executorFactory) {
    txnl = Transactional.of(executorFactory, new Supplier<ViewMds>() {
      @Override
      public ViewMds get() {
        try {
          Table table = DatasetsUtil.getOrCreateDataset(
            datasetFramework, EXPLORE_VIEW_STORE_DATASET_ID,
            "table", DatasetProperties.EMPTY,
            DatasetDefinition.NO_ARGUMENTS, null);
          return new ViewMds(new MetadataStoreDataset(table));
        } catch (Exception e) {
          LOG.error("Failed to access {} table", Constants.ConfigStore.CONFIG_TABLE, e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public boolean createOrUpdate(final Id.View viewId, final ViewProperties properties) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<ViewMds, Boolean>() {
      @Override
      public Boolean apply(ViewMds mds) throws Exception {
        boolean created = !mds.views.exists(getKey(viewId));
        mds.views.write(getKey(viewId), new ViewSpecification(viewId, properties));
        return created;
      }
    });
  }

  @Override
  public boolean exists(final Id.View viewId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<ViewMds, Boolean>() {
      @Override
      public Boolean apply(ViewMds mds) throws Exception {
        return mds.views.exists(getKey(viewId));
      }
    });
  }

  @Override
  public void delete(final Id.View viewId) throws NotFoundException {
    try {
      txnl.execute(new TransactionExecutor.Function<ViewMds, Void>() {
        @Override
        public Void apply(ViewMds mds) throws Exception {
          if (!mds.views.exists(getKey(viewId))) {
            throw new NotFoundException(viewId);
          }
          mds.views.deleteAll(getKey(viewId));
          return null;
        }
      });
    } catch (TransactionFailureException e) {
      if (e.getCause() instanceof NotFoundException) {
        throw (NotFoundException) e.getCause();
      }
      throw Throwables.propagate(e);
    } catch (InterruptedException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<String> list(final Id.Namespace namespaceId) {
    List<ViewSpecification> specs = txnl.executeUnchecked(
      new TransactionExecutor.Function<ViewMds, List<ViewSpecification>>() {
      @Override
      public List<ViewSpecification> apply(ViewMds mds) throws Exception {
        return Objects.firstNonNull(
          mds.views.<ViewSpecification>list(getKey(namespaceId), ViewSpecification.class),
          ImmutableList.<ViewSpecification>of());
      }
    });

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    builder.addAll(Collections2.transform(specs, new Function<ViewSpecification, String>() {
      @Nullable
      @Override
      public String apply(ViewSpecification input) {
        return input.getId().getId();
      }
    }));
    return builder.build();
  }

  @Override
  public ViewDetail get(final Id.View viewId) throws NotFoundException {
    try {
      ViewSpecification spec = txnl.execute(new TransactionExecutor.Function<ViewMds, ViewSpecification>() {
        @Override
        public ViewSpecification apply(ViewMds mds) throws Exception {
          return mds.views.get(getKey(viewId), ViewSpecification.class);
        }
      });
      return new ViewDetail(viewId.getId(), spec.getProperties().getSelectStatement());
    } catch (TransactionFailureException e) {
      if (e.getCause() instanceof NotFoundException) {
        throw (NotFoundException) e.getCause();
      }
      throw Throwables.propagate(e);
    } catch (InterruptedException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private MDSKey getKey(Id.Namespace id) {
    return new MDSKey.Builder()
      .add(TYPE_VIEW, id.getId())
      .build();
  }

  private MDSKey getKey(Id.View id) {
    return new MDSKey.Builder()
      .add(TYPE_VIEW, id.getNamespaceId(), id.getId())
      .build();
  }

  private static final class ViewSpecification {
    private final Id.View id;
    private final ViewProperties properties;

    private ViewSpecification(Id.View id, ViewProperties properties) {
      this.id = id;
      this.properties = properties;
    }

    public Id.View getId() {
      return id;
    }

    public ViewProperties getProperties() {
      return properties;
    }
  }

  private static final class ViewMds implements Iterable<MetadataStoreDataset> {
    private final MetadataStoreDataset views;

    private ViewMds(MetadataStoreDataset views) {
      this.views = views;
    }

    @Override
    public Iterator<MetadataStoreDataset> iterator() {
      return Iterators.singletonIterator(views);
    }
  }
}
