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

package co.cask.cdap.runtime;

import co.cask.cdap.DummyAppWithTrackingTable;
import co.cask.cdap.TrackingTable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.stream.StreamEventCodec;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * tests that flowlets and batch jobs close their data sets.
 */
@Category(XSlowTests.class)
public class OpenCloseDataSetTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static Location namespaceHomeLocation;

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {

    @Override
    public File get() {
      try {
        return tmpFolder.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  @BeforeClass
  public static void setup() throws IOException {
    NamespacedLocationFactory namespacedLocationFactory =
      AppFabricTestHelper.getInjector().getInstance(NamespacedLocationFactory.class);
    namespaceHomeLocation = namespacedLocationFactory.get(DefaultId.NAMESPACE);
    Locations.mkdirsIfNotExists(namespaceHomeLocation);
  }

  @Test(timeout = 120000)
  public void testDataSetsAreClosed() throws Exception {
    final String tableName = "foo";

    TrackingTable.resetTracker();
    ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(DummyAppWithTrackingTable.class,
                                                                                   TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);
    List<ProgramController> controllers = Lists.newArrayList();

    // start the programs
    for (Program program : app.getPrograms()) {
      if (program.getType().equals(ProgramType.MAPREDUCE)) {
        continue;
      }
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                     RunIds.generate().getId()));
      controllers.add(runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs,
                                                                   new BasicArguments())));
    }

    // write some data to queue
    TransactionSystemClient txSystemClient = AppFabricTestHelper.getInjector().
      getInstance(TransactionSystemClient.class);

    QueueName queueName = QueueName.fromStream(app.getId().getNamespaceId(), "xx");
    QueueClientFactory queueClientFactory = AppFabricTestHelper.getInjector().getInstance(QueueClientFactory.class);
    QueueProducer producer = queueClientFactory.createProducer(queueName);

    // start tx to write in queue in tx
    Transaction tx = txSystemClient.startShort();
    ((TransactionAware) producer).startTx(tx);

    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 4; i++) {
      String msg = "x" + i;
      StreamEvent event = new StreamEvent(ImmutableMap.<String, String>of(),
                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      producer.enqueue(new QueueEntry(codec.encodePayload(event)));
    }

    // commit tx
    ((TransactionAware) producer).commitTx();
    txSystemClient.commit(tx);

    while (TrackingTable.getTracker(tableName, "write") < 4) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // get the number of writes to the foo table
    Assert.assertEquals(4, TrackingTable.getTracker(tableName, "write"));
    // only the flow has started with s single flowlet (service is loaded lazily on 1st request)
    Assert.assertEquals(1, TrackingTable.getTracker(tableName, "open"));

    // now send a request to the service
    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
      getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("service.%s.%s.%s", DefaultId.NAMESPACE.getId(), "dummy", "DummyService")).iterator().next();

    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(String.format("http://%s:%d/v3/namespaces/default/apps/%s/services/%s/methods/%s",
                                            discoverable.getSocketAddress().getHostName(),
                                            discoverable.getSocketAddress().getPort(),
                                            "dummy",
                                            "DummyService",
                                            "x1"));
    HttpResponse response = client.execute(get);
    String responseContent = gson.fromJson(
      new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8), String.class);
    client.getConnectionManager().shutdown();
    Assert.assertEquals("x1", responseContent);

    // now the dataset must have a read and another open operation
    Assert.assertEquals(1, TrackingTable.getTracker(tableName, "read"));
    Assert.assertEquals(2, TrackingTable.getTracker(tableName, "open"));
    Assert.assertEquals(0, TrackingTable.getTracker(tableName, "close"));

    // stop all programs, they should both close the data set foo
    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
    int timesOpened = TrackingTable.getTracker(tableName, "open");
    Assert.assertTrue(timesOpened >= 2);
    Assert.assertEquals(timesOpened, TrackingTable.getTracker(tableName, "close"));

    // now start the m/r job
    ProgramController controller = null;
    for (Program program : app.getPrograms()) {
      if (program.getType().equals(ProgramType.MAPREDUCE)) {
        ProgramRunner runner = runnerFactory.create(
          ProgramRunnerFactory.Type.valueOf(program.getType().name()));
        BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                       RunIds.generate().getId()));
        controller = runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs, new BasicArguments()));
      }
    }
    Assert.assertNotNull(controller);

    while (!controller.getState().equals(ProgramController.State.COMPLETED)) {
      TimeUnit.MILLISECONDS.sleep(100);
    }

    // M/r job is done, one mapper and the m/r client should have opened and closed the data set foo
    // we don't know the exact number of times opened, but it is at least once, and it must be closed the same number
    // of times.
    Assert.assertTrue(timesOpened < TrackingTable.getTracker(tableName, "open"));
    Assert.assertEquals(TrackingTable.getTracker(tableName, "open"),
                        TrackingTable.getTracker(tableName, "close"));
    Assert.assertTrue(0 < TrackingTable.getTracker("bar", "open"));
    Assert.assertEquals(TrackingTable.getTracker("bar", "open"),
                        TrackingTable.getTracker("bar", "close"));

  }

  @AfterClass
  public static void tearDown() throws IOException {
    Locations.deleteQuietly(namespaceHomeLocation, true);
  }
}
