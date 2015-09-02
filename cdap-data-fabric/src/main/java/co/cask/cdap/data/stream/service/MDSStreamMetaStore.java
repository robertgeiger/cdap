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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamViewProperties;
import co.cask.cdap.proto.StreamViewSpecification;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link StreamMetaStore} that access MDS directly.
 */
public final class MDSStreamMetaStore implements StreamMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(MDSStreamMetaStore.class);

  // note: these constants should be same as in DefaultStore - this needs refactoring, but currently these pieces
  // dependent
  private static final String STREAM_META_TABLE = "app.meta";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_STREAM_VIEW = "stream-view";

  private Transactional<StreamMds, MetadataStoreDataset> txnl;

  @Inject
  public MDSStreamMetaStore(TransactionExecutorFactory txExecutorFactory, final DatasetFramework dsFramework) {
    txnl = Transactional.of(txExecutorFactory, new Supplier<StreamMds>() {
      @Override
      public StreamMds get() {
        try {
          Id.DatasetInstance streamMetaDatasetInstanceId = Id.DatasetInstance.from(Id.Namespace.SYSTEM,
                                                                                   STREAM_META_TABLE);
          Table mdsTable = DatasetsUtil.getOrCreateDataset(dsFramework, streamMetaDatasetInstanceId, "table",
                                                           DatasetProperties.EMPTY,
                                                           DatasetDefinition.NO_ARGUMENTS, null);

          return new StreamMds(new MetadataStoreDataset(mdsTable));
        } catch (Exception e) {
          LOG.warn("Failed to access app.meta table {}", e.getMessage());
          LOG.debug("Failed to access app.meta table", e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public void addStreamView(final Id.Stream.View viewId, final StreamViewProperties properties) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Void>() {
      @Override
      public Void apply(StreamMds mds) throws Exception {
        mds.streams.write(getKey(viewId), new StreamViewSpecification(viewId.getId(), properties));
        return null;
      }
    });
  }

  @Override
  public void removeStreamView(final Id.Stream.View viewId) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Void>() {
      @Override
      public Void apply(StreamMds mds) throws Exception {
        mds.streams.deleteAll(getKey(viewId));
        return null;
      }
    });
  }

  @Override
  public List<StreamViewSpecification> listStreamViews(final Id.Namespace namespaceId) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, List<StreamViewSpecification>>() {
      @Override
      public List<StreamViewSpecification> apply(StreamMds mds) throws Exception {
        return mds.streams.list(new MDSKey.Builder().add(TYPE_STREAM_VIEW, namespaceId.getId()).build(),
                                StreamViewSpecification.class);
      }
    });
  }

  @Override
  public void addStream(final Id.Stream streamId) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Void>() {
      @Override
      public Void apply(StreamMds mds) throws Exception {
        mds.streams.write(getKey(streamId),
                          createStreamSpec(streamId));
        return null;
      }
    });
  }

  @Override
  public void removeStream(final Id.Stream streamId) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Void>() {
      @Override
      public Void apply(StreamMds mds) throws Exception {
        mds.streams.deleteAll(getKey(streamId));
        return null;
      }
    });
  }

  @Override
  public boolean streamExists(final Id.Stream streamId) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Boolean>() {
      @Override
      public Boolean apply(StreamMds mds) throws Exception {
        return mds.streams.getFirst(getKey(streamId), StreamSpecification.class) != null;
      }
    });
  }

  @Override
  public List<StreamSpecification> listStreams(final Id.Namespace namespaceId) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, List<StreamSpecification>>() {
      @Override
      public List<StreamSpecification> apply(StreamMds mds) throws Exception {
        return mds.streams.list(new MDSKey.Builder().add(TYPE_STREAM, namespaceId.getId()).build(),
                                StreamSpecification.class);
      }
    });
  }

  @Override
  public Multimap<Id.Namespace, StreamSpecification> listStreams() throws Exception {
    return txnl.executeUnchecked(
      new TransactionExecutor.Function<StreamMds, Multimap<Id.Namespace, StreamSpecification>>() {
        @Override
        public Multimap<Id.Namespace, StreamSpecification> apply(StreamMds mds) throws Exception {
          ImmutableMultimap.Builder<Id.Namespace, StreamSpecification> builder = ImmutableMultimap.builder();
          Map<MDSKey, StreamSpecification> streamSpecs =
            mds.streams.listKV(new MDSKey.Builder().add(TYPE_STREAM).build(),
                               StreamSpecification.class);
          for (Map.Entry<MDSKey, StreamSpecification> streamSpecEntry : streamSpecs.entrySet()) {
            MDSKey.Splitter splitter = streamSpecEntry.getKey().split();
            // skip the first name ("stream")
            splitter.skipString();
            // Namespace id is the next part.
            String namespaceId = splitter.getString();
            builder.put(Id.Namespace.from(namespaceId), streamSpecEntry.getValue());
          }
          return builder.build();
        }
      });
  }

  private MDSKey getKey(Id.Stream.View viewId) {
    return new MDSKey.Builder()
      .add(TYPE_STREAM_VIEW, viewId.getNamespaceId(), viewId.getId()).build();
  }

  private MDSKey getKey(Id.Stream streamId) {
    return new MDSKey.Builder()
      .add(TYPE_STREAM, streamId.getNamespaceId(), streamId.getId()).build();
  }

  private StreamSpecification createStreamSpec(Id.Stream streamId) {
    return new StreamSpecification.Builder().setName(streamId.getId()).create();
  }

  private static final class StreamMds implements Iterable<MetadataStoreDataset> {
    private final MetadataStoreDataset streams;

    private StreamMds(MetadataStoreDataset metaTable) {
      this.streams = metaTable;
    }

    @Override
    public Iterator<MetadataStoreDataset> iterator() {
      return Iterators.singletonIterator(streams);
    }
  }
}
