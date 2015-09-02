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
import co.cask.cdap.common.utils.ProjectInfo;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateViewStatementBuilderTest {

  @Test
  public void testCreate() throws Exception {
    String expected = "CREATE VIEW IF NOT EXISTS view_purchases " +
      "(f1 string, f2 int, f3 double, f4 boolean, f5 float, f6 binary) COMMENT 'CDAP View!' " +
      "TBLPROPERTIES ('somekey'='someval', 'cdap.name'='purchases', " +
                      "'cdap.version'='" + ProjectInfo.getVersion().toString() + "') " +
      "AS select * from stream_z";
    Schema schema = Schema.recordOf(
      "stuff",
      Schema.Field.of("f1", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
      Schema.Field.of("f3", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("f4", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("f5", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("f6", Schema.of(Schema.Type.BYTES)));

    String actual = new CreateViewStatementBuilder("purchases", "view_purchases", "select * from stream_z")
      .setSchema(schema)
      .setTableProperties(ImmutableMap.of("somekey", "someval"))
      .setTableComment("CDAP View!")
      .build();
    Assert.assertEquals(expected, actual);
  }
}
