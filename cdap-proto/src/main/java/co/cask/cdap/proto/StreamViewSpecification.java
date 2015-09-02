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
package co.cask.cdap.proto;

import co.cask.cdap.api.data.format.FormatSpecification;

import java.util.Objects;

/**
 * Represents a stream view.
 */
public class StreamViewSpecification {

  private final String id;
  private final FormatSpecification format;

  public StreamViewSpecification(String id, StreamViewProperties properties) {
    this(id, properties.getFormat());
  }

  public StreamViewSpecification(String id, FormatSpecification format) {
    this.id = id;
    this.format = format;
  }

  public FormatSpecification getFormat() {
    return format;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamViewSpecification that = (StreamViewSpecification) o;
    return Objects.equals(id, that.id) && Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, format);
  }
}
