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

package co.cask.cdap.explore.executor;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.proto.internal.CreateTableRequest;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.http.HttpHandler;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.matcher.Matchers;
import org.junit.Test;

import java.net.URL;

/**
 *
 */
public class ExploreTableHandlerTest extends HandlerTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  @Override
  protected Iterable<? extends HttpHandler> getHandlers() {
    TestExploreTableManager testManager = new TestExploreTableManager();
    return ImmutableList.of(new ExploreTableHandler(testManager));
  }

  @Test
  public void testAll() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;
    Id.Table table1 = Id.Table.from(namespace, "table1");

    Schema schema = Schema.recordOf("sdf", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    CreateTableRequest tableRequest = CreateTableRequest.builder("table1", "SomeStorageHandler")
      .setSchema(schema)
      .build();

    URL path = resolve("/v3/namespaces/%s/data/explore/tables/%s", table1.getNamespaceId(), table1.getId());
    execute(
      HttpRequest.post(path).withBody(GSON.toJson(tableRequest)).build(),
      Matchers.only(200));

    execute(
      HttpRequest.delete(path).build(),
      Matchers.only(200));
  }
}
