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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamViewProperties;
import co.cask.cdap.proto.StreamViewSpecification;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * In-memory implementation of the {@link StreamMetaStore}. Used for testing.
 */
public class InMemoryStreamMetaStore implements StreamMetaStore {

  private final Multimap<String, String> views;
  private final Map<Id.Stream.View, StreamViewProperties> viewProperties;
  private final Multimap<String, String> streams;

  public InMemoryStreamMetaStore() {
    this.streams = Multimaps.synchronizedMultimap(HashMultimap.<String, String>create());
    this.views = HashMultimap.create();
    this.viewProperties = Maps.newHashMap();
  }

  @Override
  public void addStreamView(Id.Stream.View viewId, StreamViewProperties properties) throws Exception {
    synchronized (views) {
      views.put(viewId.getNamespaceId(), viewId.getId());
      viewProperties.put(viewId, properties);
    }
  }

  @Override
  public void removeStreamView(Id.Stream.View viewId) throws Exception {
    synchronized (views) {
      views.remove(viewId.getNamespaceId(), viewId.getId());
      viewProperties.remove(viewId);
    }
  }

  @Override
  public List<StreamViewSpecification> listStreamViews(final Id.Namespace namespaceId) throws Exception {
    ImmutableList.Builder<StreamViewSpecification> builder = ImmutableList.builder();
    builder.addAll(Collections2.transform(views.values(), new Function<String, StreamViewSpecification>() {
      @Nullable
      @Override
      public StreamViewSpecification apply(@Nullable String input) {
        return new StreamViewSpecification(input, viewProperties.get(Id.Stream.View.from(namespaceId, input)));
      }
    }));
    return builder.build();
  }

  @Override
  public StreamViewProperties getStreamView(Id.Stream.View viewId) {
    return viewProperties.get(viewId);
  }

  @Override
  public List<StreamViewSpecification> listStreamViews(Id.Stream streamId) {
    ImmutableList.Builder<StreamViewSpecification> builder = ImmutableList.builder();
    builder.addAll(Collections2.transform(
      Collections2.filter(viewProperties.entrySet(), new Predicate<Map.Entry<Id.Stream.View, StreamViewProperties>>() {
                            @Override
                            public boolean apply(@Nullable Map.Entry<Id.Stream.View, StreamViewProperties> input) {
                              return false;
                            }
                          },
                          new Function<Map.Entry<Id.Stream.View, StreamViewProperties>, StreamViewSpecification>() {
                            @Nullable
                            @Override
                            public StreamViewSpecification apply(@Nullable Map.Entry<Id.Stream.View, StreamViewProperties> input) {
                              return new StreamViewSpecification();
                            }
                          }));
    return builder.build();
  }

  @Override
  public void addStream(Id.Stream streamId) throws Exception {
    streams.put(streamId.getNamespaceId(), streamId.getId());
  }

  @Override
  public void removeStream(Id.Stream streamId) throws Exception {
    streams.remove(streamId.getNamespaceId(), streamId.getId());
  }

  @Override
  public boolean streamExists(Id.Stream streamId) throws Exception {
    return streams.containsEntry(streamId.getNamespaceId(), streamId.getId());
  }

  @Override
  public List<StreamSpecification> listStreams(Id.Namespace namespaceId) throws Exception {
    ImmutableList.Builder<StreamSpecification> builder = ImmutableList.builder();
    synchronized (streams) {
      for (String stream : streams.get(namespaceId.getId())) {
        builder.add(new StreamSpecification.Builder().setName(stream).create());
      }
    }
    return builder.build();
  }

  @Override
  public synchronized Multimap<Id.Namespace, StreamSpecification> listStreams() throws Exception {
    ImmutableMultimap.Builder<Id.Namespace, StreamSpecification> builder = ImmutableMultimap.builder();
    for (String namespaceId : streams.keySet()) {
      synchronized (streams) {
        Collection<String> streamNames = streams.get(namespaceId);
        builder.putAll(Id.Namespace.from(namespaceId),
                       Collections2.transform(streamNames, new Function<String, StreamSpecification>() {
                         @Nullable
                         @Override
                         public StreamSpecification apply(String input) {
                           return new StreamSpecification.Builder().setName(input).create();
                         }
                       }));
      }
    }
    return builder.build();
  }
}
