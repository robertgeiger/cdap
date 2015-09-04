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

package co.cask.cdap.proto.internal;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Request for creating an Explore table.
 */
public class CreateTableRequest {

  private final String name;
  private final String comment;
  private final String storageHandler;
  private final Schema schema;
  private final Map<String, String> serdeProperties;

  public CreateTableRequest(String name, String storageHandler,
                            @Nullable String comment,
                            @Nullable Schema schema,
                            @Nullable Map<String, String> serdeProperties) {
    this.name = name;
    this.comment = comment;
    this.storageHandler = storageHandler;
    this.schema = schema;
    this.serdeProperties = serdeProperties;
  }

  public static Builder builder(String name, String storageHandler) {
    return new Builder(name, storageHandler);
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getComment() {
    return comment;
  }

  public String getStorageHandler() {
    return storageHandler;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  @Nullable
  public Map<String, String> getSerdeProperties() {
    return serdeProperties;
  }

  public static class Builder {
    private final String name;
    private final String storageHandler;
    private String comment;
    private Schema schema;
    private Map<String, String> serdeProperties;

    public Builder(String name, String storageHandler) {
      this.name = name;
      this.storageHandler = storageHandler;
    }

    public Builder setComment(String comment) {
      this.comment = comment;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setSerdeProperties(Map<String, String> serdeProperties) {
      this.serdeProperties = serdeProperties;
      return this;
    }

    public CreateTableRequest build() {
      return new CreateTableRequest(name, storageHandler, comment, schema, serdeProperties);
    }
  }
}
