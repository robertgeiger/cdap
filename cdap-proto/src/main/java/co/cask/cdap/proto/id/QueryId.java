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
 *
 */
public class QueryId extends ElementId {

  private final String handle;

  public QueryId(String handle) {
    super(ElementType.QUERY);
    this.handle = handle;
  }

  public String getHandle() {
    return handle;
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
    QueryId queryId = (QueryId) o;
    return Objects.equals(handle, queryId.handle);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), handle);
  }

  @Override
  public Id toId() {
    return Id.QueryHandle.from(handle);
  }

  @SuppressWarnings("unused")
  public static QueryId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new QueryId(safeNextAndEnd(iterator, "handle"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(handle);
  }
}
