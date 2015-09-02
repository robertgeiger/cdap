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
import co.cask.cdap.api.data.schema.Schema;

import java.util.Objects;

/**
 * Represents a stream view properties.
 */
public class StreamViewProperties {

  private final Id.Stream stream;
  private final FormatSpecification format;
  private final Schema schema;

  public StreamViewProperties(Id.Stream stream, FormatSpecification format, Schema schema) {
    this.stream = stream;
    this.format = format;
    this.schema = schema;
  }

  public Id.Stream getStream() {
    return stream;
  }

  public FormatSpecification getFormat() {
    return format;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamViewProperties that = (StreamViewProperties) o;
    return Objects.equals(stream, stream) && Objects.equals(format, that.format) && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stream, format, schema);
  }
}
