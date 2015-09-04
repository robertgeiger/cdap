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

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreTableManager;
import co.cask.cdap.proto.internal.CreateTableRequest;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import com.google.common.collect.Maps;

import java.sql.SQLException;
import java.util.Map;

/**
 * Test implementation of {@link ExploreTableManager}.
 */
public class TestExploreTableManager extends ExploreTableManager {

  private Map<Id.Table, CreateTableRequest> tables;

  public TestExploreTableManager() {
    super(null);
    tables = Maps.newHashMap();
  }

  @Override
  public QueryHandle createTable(Id.Table table, CreateTableRequest request)
    throws UnsupportedTypeException, ExploreException, SQLException {
    if (tables.containsKey(table)) {
      throw new ExploreException(String.format("Table '%s' already exists", table));
    }
    tables.put(table, request);
    return QueryHandle.fromId("test");
  }

  @Override
  public QueryHandle deleteTable(Id.Table table) throws ExploreException, SQLException {
    CreateTableRequest removed = tables.remove(table);
    if (removed == null) {
      throw new ExploreException(String.format("Table '%s' does not exist", table));
    }
    return QueryHandle.fromId("test");
  }
}
