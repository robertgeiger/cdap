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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.batch.sink.BatchCassandraSink;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.cassandraunit.CassandraUnit;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *  Unit test for batch {@link BatchCassandraSink} class.
 */
public class ETLCassandraTest extends BaseETLBatchTest {
  private static final Gson GSON = new Gson();
  private static final String STREAM_NAME = "myStream";
  private static final String TABLE_NAME = "outputTable";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private int port;
  private Schema schema;

  @Before
  public void beforeTest() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    String clusterName = "TestCluster";
    String host = "localhost:9171";
  }

  @After
  public void afterTest() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  @Category(SlowTests.class)
  public void testES() throws Exception {
    testESSink();
  }

  private void testESSink() throws Exception {
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, STREAM_NAME)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("Cassandra", new ImmutableMap.Builder<String, String>()
      .put(Properties.Cassandra.INITIAL_ADDRESS, InetAddress.getLocalHost().getHostName())
      .put(Properties.Cassandra.PORT, "9171")
      .put(Properties.Cassandra.PARITIONER, "org.apache.cassandra.dht.Murmur3Partitioner")
      .put(Properties.Cassandra.KEYSPACE, "test")
      .put(Properties.Cassandra.COLUMNS, "ticker, num, price")
      .build());

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "cassandraSinkTest");
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();



//    SearchResponse searchResponse = client.prepareSearch("test").execute().actionGet();
//    Assert.assertEquals(2, searchResponse.getHits().getTotalHits());
//    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "AAPL")).execute().actionGet();
//    Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
//    Assert.assertEquals("test", searchResponse.getHits().getAt(0).getIndex());
//    Assert.assertEquals("testing", searchResponse.getHits().getAt(0).getType());
//    Assert.assertEquals("AAPL", searchResponse.getHits().getAt(0).getId());
//    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "ABCD")).execute().actionGet();
//    Assert.assertEquals(0, searchResponse.getHits().getTotalHits());
//
//    DeleteResponse response = client.prepareDelete("test", "testing", "CDAP").execute().actionGet();
//    Assert.assertTrue(response.isFound());
  }
}
