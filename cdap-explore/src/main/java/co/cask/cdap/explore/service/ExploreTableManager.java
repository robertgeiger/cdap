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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.explore.table.CreateStatementBuilder;
import co.cask.cdap.proto.internal.CreateTableRequest;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * Executes table creation, deletion, and modification.
 */
public class ExploreTableManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreTableManager.class);

  private final ExploreService exploreService;

  @Inject
  public ExploreTableManager(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  public QueryHandle createTable(Id.Table table, CreateTableRequest request)
    throws UnsupportedTypeException, ExploreException, SQLException {

    LOG.debug("Creating table {} from request:\n{}", table, request);
    CreateStatementBuilder builder = new CreateStatementBuilder(table.getId(), table.getId());
    if (request.getSchema() != null) {
      builder.setSchema(request.getSchema());
    }
    if (request.getComment() != null) {
      builder.setTableComment(request.getComment());
    }
    String statement = builder.buildWithStorageHandler(request.getStorageHandler(), request.getSerdeProperties());
    return exploreService.execute(table.getNamespace(), statement);
  }

  public QueryHandle deleteTable(Id.Table table) throws ExploreException, SQLException {
    LOG.debug("Deleting table {}", table);
    return exploreService.execute(table.getNamespace(), table.getId());
  }

  public QueryHandle addPartition(Id.Table table, PartitionKey partitionKey, String fsPath)
    throws ExploreException, SQLException {

    String addPartitionStatement = String.format(
      "ALTER TABLE %s ADD PARTITION %s LOCATION '%s'",
      table.getId(), generateHivePartitionKey(partitionKey), fsPath);

    LOG.debug("Adding partition for key {} table {}:\n{}", partitionKey, table, addPartitionStatement);
    return exploreService.execute(table.getNamespace(), addPartitionStatement);
  }

  public QueryHandle addPartitions(Id.Table table, Set<PartitionDetail> partitionDetails)
    throws ExploreException, SQLException {

    if (partitionDetails.isEmpty()) {
      return QueryHandle.NO_OP;
    }

    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE ")
      .append(table.getId())
      .append(" ADD");
    for (PartitionDetail partitionDetail : partitionDetails) {
      statement.append(" PARTITION")
        .append(generateHivePartitionKey(partitionDetail.getPartitionKey()))
        .append(" LOCATION '")
        .append(partitionDetail.getRelativePath())
        .append("'");
    }

    LOG.debug("Adding partitions for table {}", table);
    return exploreService.execute(table.getNamespace(), statement.toString());
  }

  public QueryHandle dropPartition(Id.Table table, PartitionKey partitionKey)
    throws ExploreException, SQLException {

    String dropPartitionStatement = String.format(
      "ALTER TABLE %s DROP PARTITION %s", table.getId(), generateHivePartitionKey(partitionKey));

    LOG.debug("Dropping partition for key {} table {}:\n{}", partitionKey, table, dropPartitionStatement);
    return exploreService.execute(table.getNamespace(), dropPartitionStatement);
  }

  private String generateHivePartitionKey(PartitionKey key) {
    StringBuilder builder = new StringBuilder("(");
    String sep = "";
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      String fieldName = entry.getKey();
      Comparable fieldValue = entry.getValue();
      String quote = fieldValue instanceof String ? "'" : "";
      builder.append(sep).append(fieldName).append("=").append(quote).append(fieldValue.toString()).append(quote);
      sep = ", ";
    }
    builder.append(")");
    return builder.toString();
  }
}
