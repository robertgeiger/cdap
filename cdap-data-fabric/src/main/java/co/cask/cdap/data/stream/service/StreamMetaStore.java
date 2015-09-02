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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamViewProperties;
import co.cask.cdap.proto.StreamViewSpecification;
import com.google.common.collect.Multimap;

import java.util.List;

/**
 * A temporary place for hosting MDS access logic for streams.
 */
// TODO: The whole access pattern to MDS needs to be rethink, as we are now moving towards SOA and multiple components
// needs to access MDS.
public interface StreamMetaStore {

  /**
   * Adds a stream to the meta store.
   */
  void addStream(Id.Stream streamId) throws Exception;

  /**
   * Removes a stream from the meta store.
   */
  void removeStream(Id.Stream streamId) throws Exception;

  /**
   * Checks if a stream exists in the meta store.
   */
  boolean streamExists(Id.Stream streamId) throws Exception;

  /**
   * List all stream specifications stored for the {@code namespaceId}.
   */
  List<StreamSpecification> listStreams(Id.Namespace namespaceId) throws Exception;

  /**
   * List all stream specifications with their associated {@link Id.Namespace}.
   */
  Multimap<Id.Namespace, StreamSpecification> listStreams() throws Exception;

  /**
   * Adds a stream view to the meta store.
   */
  void addStreamView(Id.Stream.View viewId, StreamViewProperties properties) throws Exception;

  /**
   * Removes a stream view from the meta store.
   */
  void removeStreamView(Id.Stream.View viewId) throws Exception;

  /**
   * Lists all stream views stored for the {@code namespaceId}.
   */
  List<StreamViewSpecification> listStreamViews(Id.Namespace namespaceId) throws Exception;

  /**
   * Gets the properties of a stream view.
   */
  StreamViewProperties getStreamView(Id.Stream.View viewId);

  /**
   * Lists all views associated with a stream.
   */
  List<StreamViewSpecification> listStreamViews(Id.Stream streamId);
}
