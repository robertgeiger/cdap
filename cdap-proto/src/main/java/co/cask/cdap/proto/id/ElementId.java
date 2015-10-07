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
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a particular instance of an element.
 */
@SuppressWarnings("unchecked")
public abstract class ElementId implements IdCompatible {

  private final ElementType elementType;

  protected ElementId(ElementType elementType) {
    this.elementType = elementType;
  }

  protected abstract Iterable<String> toIdParts();

  public final ElementType getElementType() {
    return elementType;
  }

  public static <T extends Id> T fromStringOld(String string, Class<T> oldIdClass) {
    ElementType type = ElementType.valueOfOldIdClass(oldIdClass);
    ElementId id = fromString(string, type.getIdClass());
    return (T) id.toId();
  }

  public static <T extends ElementId> T fromString(String string, Class<T> idClass) {
    String[] typeAndId = string.split(":", 2);
    Preconditions.checkArgument(typeAndId.length == 2, "Expected separator ':' to be in the string");

    String typeString = typeAndId[0];
    ElementType type = ElementType.valueOf(typeString.toUpperCase());
    Preconditions.checkArgument(type != null, "Invalid element type: " + typeString);
    Preconditions.checkArgument(
      type.getIdClass().equals(idClass),
      "Expected ElementId of class '%s' but got '%s'",
      idClass.getName(), type.getIdClass().getName());

    String idString = typeAndId[1];
    return type.fromIdParts(Splitter.on(":").split(idString));
  }

  @Override
  public final String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(elementType.name().toLowerCase()).append(':');
    for (String part : toIdParts()) {
      builder.append(part).append(':');
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ElementId elementId = (ElementId) o;
    return Objects.equals(elementType, elementId.elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementType);
  }

  protected static String safeNext(Iterator<String> iterator, String fieldName) {
    Preconditions.checkArgument(iterator.hasNext(), "Missing field: %s", fieldName);
    return iterator.next();
  }

  protected static String safeNextAndEnd(Iterator<String> iterator, String fieldName) {
    String result = safeNext(iterator, fieldName);
    Preconditions.checkArgument(!iterator.hasNext(), "Expected end");
    return result;
  }
}
