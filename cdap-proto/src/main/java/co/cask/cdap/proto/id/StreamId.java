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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.ElementType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a stream.
 */
public class StreamId extends ElementId implements NamespacedId, ParentedId<NamespaceId> {
  private final String namespace;
  private final String stream;

  public StreamId(String namespace, String stream) {
    super(ElementType.STREAM);
    this.namespace = namespace;
    this.stream = stream;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getStream() {
    return stream;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    StreamId streamId = (StreamId) o;
    return Objects.equals(namespace, streamId.namespace) &&
      Objects.equals(stream, streamId.stream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, stream);
  }

  @Override
  public Id toId() {
    return Id.Stream.from(namespace, stream);
  }

  @SuppressWarnings("unused")
  public static StreamId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new StreamId(safeNext(iterator, "namespace"), safeNextAndEnd(iterator, "stream"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, stream);
  }
}
