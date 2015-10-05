/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Store for namespace metadata.
 */
public class NamespaceMetadataStore extends MetadataStoreDataset {

  private static final Gson GSON = new Gson();
  private static final String TYPE_NAMESPACE = "namespace";

  public NamespaceMetadataStore(Table table) {
    super(table);
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(byte[] serialized, Type typeOfT) {
    return GSON.fromJson(Bytes.toString(serialized), typeOfT);
  }

  public void create(NamespaceMeta metadata) {
    write(getKey(metadata.getName()), metadata);
  }

  public NamespaceMeta get(Id.Namespace id) {
    return getFirst(getKey(id.getId()), NamespaceMeta.class);
  }

  public void delete(Id.Namespace id) {
    deleteAll(getKey(id.getId()));
  }

  public List<NamespaceMeta> list() {
    return list(getKey(null), NamespaceMeta.class);
  }

  private MDSKey getKey(@Nullable String name) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    if (null != name) {
      builder.add(name);
    }
    return builder.build();
  }
}
