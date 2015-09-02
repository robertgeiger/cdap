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

package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.StreamViewProperties;
import co.cask.cdap.proto.StreamViewSpecification;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 *
 */
public interface StreamAdmin {

  /**
   * Deletes all entries for all streams within a namespace
   */
  void dropAllInNamespace(Id.Namespace namespace) throws Exception;

  /**
   * Sets the number of consumer instances for the given consumer group in a stream.
   *
   * @param streamId Id of the stream.
   * @param groupId The consumer group to alter.
   * @param instances Number of instances.
   */
  void configureInstances(Id.Stream streamId, long groupId, int instances) throws Exception;

  /**
   * Sets the consumer groups information for the given stream.
   *
   * @param streamId Id of the stream.
   * @param groupInfo A map from groupId to number of instances of each group.
   */
  void configureGroups(Id.Stream streamId, Map<Long, Integer> groupInfo) throws Exception;

  /**
   * Performs upgrade action for all streams.
   */
  void upgrade() throws Exception;

  /**
   * Returns the configuration of the given stream.
   * @param streamId Id of the stream.
   * @return A {@link StreamConfig} instance.
   * @throws IOException If the stream doesn't exists.
   */
  StreamConfig getConfig(Id.Stream streamId) throws IOException;

  /**
   * Overwrites existing configuration for the given stream.
   *
   * @param streamId Id of the stream whose properties are being updated
   * @param properties New configuration of the stream.
   */
  void updateConfig(Id.Stream streamId, StreamProperties properties) throws IOException;

  /**
   * @param streamId Id of the stream.
   * @return true if stream with given Id exists, otherwise false
   * @throws Exception if check fails
   */
  boolean exists(Id.Stream streamId) throws Exception;

  /**
   * Creates stream if doesn't exist. If stream exists does nothing.
   *
   * @param streamId Id of the stream to create
   * @throws Exception if creation fails
   */
  void create(Id.Stream streamId) throws Exception;

  /**
   * Creates stream if doesn't exist. If stream exists, does nothing.
   *
   * @param streamId Id of the stream to create
   * @param props additional properties
   * @throws Exception if creation fails
   */
  void create(Id.Stream streamId, @Nullable Properties props) throws Exception;

  /**
   * Wipes out stream data.
   *
   * @param streamId Id of the stream to truncate
   * @throws Exception if cleanup fails
   */
  void truncate(Id.Stream streamId) throws Exception;

  /**
   * Deletes stream from the system completely.
   *
   * @param streamId Id of the stream to delete
   * @throws Exception if deletion fails
   */
  void drop(Id.Stream streamId) throws Exception;

  /**
   * Register stream used by program.
   *
   * @param owners the ids that are using the stream
   * @param streamId the stream being used
   */
  void register(Iterable<? extends Id> owners, Id.Stream streamId);

  /**
   * Creates a stream view if it doesn't exist. If stream view exists, update the existing stream view.
   *
   * @param viewId the stream view
   * @param properties the stream view properties
   * @return true if a new stream view was created, false if updated
   */
  boolean createOrUpdateView(Id.Stream.View viewId, StreamViewProperties properties) throws Exception;

  /**
   * Deletes a stream view.
   *
   * @param viewId the stream view
   */
  void deleteView(Id.Stream.View viewId) throws Exception;

  /**
   * Gets a stream view.
   *
   * @param viewId the stream view
   */
  StreamViewProperties getView(Id.Stream.View viewId);

  /**
   * Lists views for a stream.
   *
   * @param streamId the stream
   * @return the list of views for the stream
   */
  List<StreamViewSpecification> listViews(Id.Stream streamId);

  /**
   * Lists views for a namespace.
   *
   * @param namespace the namespace
   * @return the list of views for the namespace
   */
  List<StreamViewSpecification> listViews(Id.Namespace namespace) throws Exception;
}
