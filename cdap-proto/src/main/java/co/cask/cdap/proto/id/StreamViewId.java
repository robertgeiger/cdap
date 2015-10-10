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
 * Uniquely identifies a stream view.
 */
public class StreamViewId extends ElementId implements NamespacedId, ParentedId<StreamId> {
  private final String namespace;
  private final String stream;
  private final String view;

  public StreamViewId(String namespace, String stream, String view) {
    super(ElementType.STREAM_VIEW);
    this.namespace = namespace;
    this.stream = stream;
    this.view = view;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getStream() {
    return stream;
  }

  public String getView() {
    return view;
  }

  @Override
  public StreamId getParent() {
    return new StreamId(namespace, stream);
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
    StreamViewId that = (StreamViewId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(stream, that.stream) &&
      Objects.equals(view, that.view);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, stream, view);
  }

  @Override
  public Id toId() {
    return Id.Stream.View.from(namespace, stream, view);
  }

  @SuppressWarnings("unused")
  public static StreamViewId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new StreamViewId(
      safeNext(iterator, "namespace"), safeNext(iterator, "stream"),
      safeNextAndEnd(iterator, "view"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, stream);
  }
}
