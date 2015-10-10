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
package co.cask.cdap.proto.element;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.ElementId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.FlowletQueueId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.QueryId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.id.SystemServiceId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;

/**
 * Represents a type of CDAP element. E.g. namespace, application, datasets, streams.
 */
// TODO: remove duplication with ElementType in cdap-cli
@SuppressWarnings("unchecked")
public enum ElementType {

  NAMESPACE(NamespaceId.class, Id.Namespace.class),
  APPLICATION(ApplicationId.class, Id.Application.class),
  PROGRAM(ProgramId.class, Id.Program.class),
  PROGRAM_RUN(ProgramRunId.class, Id.Program.Run.class),

  STREAM(StreamId.class, Id.Stream.class),
  STREAM_VIEW(StreamViewId.class, Id.Stream.View.class),

  DATASET_TYPE(DatasetTypeId.class, Id.DatasetType.class),
  DATASET_MODULE(DatasetModuleId.class, Id.DatasetModule.class),
  FLOWLET(FlowletId.class, Id.Flow.Flowlet.class),
  FLOWLET_QUEUE(FlowletQueueId.class, Id.Flow.Flowlet.Queue.class),
  SCHEDULE(ScheduleId.class, Id.Schedule.class),
  NOTIFICATION_FEED(NotificationFeedId.class, Id.NotificationFeed.class),
  ARTIFACT(NamespacedArtifactId.class, Id.Artifact.class),
  DATASET(DatasetId.class, Id.DatasetInstance.class),

  QUERY(QueryId.class, Id.QueryHandle.class),
  SYSTEM_SERVICE(SystemServiceId.class, Id.SystemService.class);

  private static final Map<Class<? extends ElementId>, ElementType> byIdClass;
  private static final Map<Class<? extends Id>, ElementType> byOldIdClass;
  static {
    ImmutableMap.Builder<Class<? extends ElementId>, ElementType> builder = ImmutableMap.builder();
    ImmutableMap.Builder<Class<? extends Id>, ElementType> builderOld = ImmutableMap.builder();
    for (ElementType type : ElementType.values()) {
      builder.put(type.getIdClass(), type);
      builderOld.put(type.getOldIdClass(), type);
    }
    byIdClass = builder.build();
    byOldIdClass = builderOld.build();
  }

  private final Class<? extends ElementId> idClass;
  private final Class<? extends Id> oldIdClass;
  private final MethodHandle fromIdParts;

  ElementType(Class<? extends ElementId> idClass, Class<? extends Id> oldIdClass) {
    this.idClass = idClass;
    this.oldIdClass = oldIdClass;
    try {
      this.fromIdParts = MethodHandles.lookup()
        .findStatic(idClass, "fromIdParts", MethodType.methodType(idClass, Iterable.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException("Failed to initialize ElementType", e);
    }
  }

  public Class<? extends ElementId> getIdClass() {
    return idClass;
  }

  public Class<? extends Id> getOldIdClass() {
    return oldIdClass;
  }

  public static ElementType valueOfIdClass(Class<? extends ElementId> idClass) {
    if (!byIdClass.containsKey(idClass)) {
      throw new IllegalArgumentException("No ElementType registered for ID class: " + idClass.getName());
    }
    return byIdClass.get(idClass);
  }

  public static ElementType valueOfOldIdClass(Class<? extends Id> oldIdClass) {
    if (!byOldIdClass.containsKey(oldIdClass)) {
      throw new IllegalArgumentException("No ElementType registered for old ID class: " + oldIdClass.getName());
    }
    return byOldIdClass.get(oldIdClass);
  }

  public <T extends ElementId> T fromIdParts(Iterable<String> idParts) {
    try {
      return (T) fromIdParts.invoke(idParts);
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }
}
