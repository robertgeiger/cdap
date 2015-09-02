/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.inmemory;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.StreamViewProperties;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * admin for queues in memory.
 */
@Singleton
public class InMemoryStreamAdmin extends InMemoryQueueAdmin implements StreamAdmin {
  private final StreamMetaStore streamMetaStore;
  private final UsageRegistry usageRegistry;

  @Inject
  public InMemoryStreamAdmin(InMemoryQueueService queueService,
                             UsageRegistry usageRegistry,
                             StreamMetaStore streamMetaStore) {
    super(queueService);
    this.usageRegistry = usageRegistry;
    this.streamMetaStore = streamMetaStore;
  }

  @Override
  public void dropAllInNamespace(Id.Namespace namespace) throws Exception {
    queueService.resetStreamsWithPrefix(QueueName.prefixForNamedspacedStream(namespace.getId()));
    for (StreamSpecification spec : streamMetaStore.listStreams(namespace)) {
      streamMetaStore.removeStream(Id.Stream.from(namespace, spec.getName()));
    }
  }

  @Override
  public void configureInstances(Id.Stream streamId, long groupId, int instances) throws Exception {
    // No-op
  }

  @Override
  public void configureGroups(Id.Stream streamId, Map<Long, Integer> groupInfo) throws Exception {
    // No-op
  }

  @Override
  public StreamConfig getConfig(Id.Stream streamId) {
    throw new UnsupportedOperationException("Stream config not supported for non-file based stream.");
  }

  @Override
  public void updateConfig(Id.Stream streamId, StreamProperties properties) throws IOException {
    throw new UnsupportedOperationException("Stream config not supported for non-file based stream.");
  }

  @Override
  public boolean exists(Id.Stream streamId) throws Exception {
    return exists(QueueName.fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId) throws Exception {
    create(QueueName.fromStream(streamId));
  }

  @Override
  public void create(Id.Stream streamId, @Nullable Properties props) throws Exception {
    create(QueueName.fromStream(streamId), props);
    streamMetaStore.addStream(streamId);
  }

  @Override
  public void truncate(Id.Stream streamId) throws Exception {
    truncate(QueueName.fromStream(streamId));
  }

  @Override
  public void drop(Id.Stream streamId) throws Exception {
    drop(QueueName.fromStream(streamId));
    streamMetaStore.removeStream(streamId);

    TODO: remove associated views
  }

  @Override
  public void register(Iterable<? extends Id> owners, Id.Stream streamId) {
    usageRegistry.registerAll(owners, streamId);
  }
}
