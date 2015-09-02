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

package co.cask.cdap.explore.table;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Builds create view statements for Hive. Table DDL we support is of the form:
 *
 * CREATE VIEW [IF NOT EXISTS] [db_name.]view_name [(column_name [COMMENT column_comment], ...) ]
 *   [COMMENT view_comment]
 *   [TBLPROPERTIES (property_name = property_value, ...)]
 *   AS SELECT ...;
 */
public class CreateViewStatementBuilder {

  private final String name;
  private final String hiveViewName;
  private final String selectStatement;

  private String hiveSchema;
  private String tableComment;
  private Map<String, String> tableProperties;
  private boolean orReplace;

  public CreateViewStatementBuilder(String name, String hiveViewName, String selectStatement) {
    this.name = name;
    this.hiveViewName = hiveViewName;
    this.selectStatement = selectStatement;
    this.tableProperties = addRequiredTableProperties(Maps.<String, String>newHashMap());
  }

  /**
   * Set orReplace.
   * If true, the statement starts with "CREATE OR REPLACE VIEW".
   * If false, the statement starts with "CREATE VIEW".
   */
  public CreateViewStatementBuilder setOrReplace(boolean orReplace) {
    this.orReplace = orReplace;
    return this;
  }

  /**
   * Set the schema for the table. Throws an exception if it is not valid for Hive.
   */
  public CreateViewStatementBuilder setSchema(Schema schema) throws UnsupportedTypeException {
    this.hiveSchema = SchemaConverter.toHiveSchema(schema);
    return this;
  }

  /**
   * Set the hive schema for the table. Should be of the form "column_name column_type, ...".
   */
  public CreateViewStatementBuilder setSchema(String hiveSchema) {
    this.hiveSchema = "(" + hiveSchema + ")";
    return this;
  }


  /**
   * Set table properties. CDAP name and version must not be in the given properties, as they are added by the builder.
   */
  public CreateViewStatementBuilder setTableProperties(Map<String, String> tableProperties) {
    this.tableProperties = addRequiredTableProperties(tableProperties);
    return this;
  }

  /**
   * Set a comment for the Hive view.
   */
  public CreateViewStatementBuilder setTableComment(String tableComment) {
    this.tableComment = tableComment;
    return this;
  }

  // required properties for every CDAP Hive view
  private Map<String, String> addRequiredTableProperties(Map<String, String> map) {
    return ImmutableMap.<String, String>builder().putAll(map)
      .put(Constants.Explore.CDAP_NAME, name)
      .put(Constants.Explore.CDAP_VERSION, ProjectInfo.getVersion().toString())
      .build();
  }

  /**
   * Start the create statement.
   *
   * CREATE VIEW [IF NOT EXISTS] [db_name.]view_name [(column_name [COMMENT column_comment], ...) ]
   *   [COMMENT view_comment]
   *   [TBLPROPERTIES (property_name = property_value, ...)]
   *   AS SELECT ...;
   */
  public String build() {
    StringBuilder strBuilder = new StringBuilder();

    strBuilder.append("CREATE");
    if (orReplace) {
      strBuilder.append(" OR REPLACE");
    }
    strBuilder.append(" VIEW IF NOT EXISTS ").append(hiveViewName);

    // yeah... schema is not always required.
    if (hiveSchema != null) {
      strBuilder.append(" ").append(hiveSchema);
    }

    if (tableComment != null && !tableComment.isEmpty()) {
      strBuilder.append(" COMMENT '")
        .append(tableComment)
        .append("'");
    }

    // table properties is never empty because of required cdap properties
    strBuilder.append(" TBLPROPERTIES ");
    appendMap(strBuilder, tableProperties);

    strBuilder.append(" AS ").append(selectStatement);
    return strBuilder.toString();
  }

  // appends the contents of the map as ('key'='val', ...). Also escapes any single quotes in the map.
  private void appendMap(StringBuilder strBuilder, Map<String, String> map) {
    strBuilder.append("(");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      strBuilder.append("'")
        .append(entry.getKey().replaceAll("'", "\\\\'"))
        .append("'='")
        .append(entry.getValue().replaceAll("'", "\\\\'"))
        .append("', ");
    }
    // remove trailing ", "
    strBuilder.deleteCharAt(strBuilder.length() - 1)
      .deleteCharAt(strBuilder.length() - 1)
      .append(")");
  }

}
