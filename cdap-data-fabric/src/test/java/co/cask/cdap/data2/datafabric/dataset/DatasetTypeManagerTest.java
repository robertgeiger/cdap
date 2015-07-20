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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetServiceTestBase;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import org.apache.twill.internal.Services;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Testing DatasetTypeManager class.
 */
public class DatasetTypeManagerTest extends DatasetServiceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeManagerTest.class);

  private DatasetTypeManager datasetTypeManager;
  private CConfiguration cConf;

  @Before
  public void before() throws Exception {
    super.before();

    // Boiler plate setup for Dataset manager test.
    cConf = CConfiguration.create();
    File dataDir = new File(tmpFolder.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());
    if (!DirUtils.mkdirs(dataDir)) {
      throw new RuntimeException(String.format("Could not create DatasetFramework output dir %s", dataDir));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, dataDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    // Set dataset ext modules
    cConf.set(DatasetTypeManager.CDAP_DATASET_EXT_MODULES, FakeExtDatasetModule.NAME + ":" +
      FakeExtDatasetModule.class.getName());
  }

  @After
  public void after() {
    super.after();

    Services.chainStop(datasetTypeManager);
  }

  @Test
  public void testExtDatesetModule() {
    datasetTypeManager = new DatasetTypeManager(cConf, mdsDatasetsRegistry, locationFactory,
                                                Collections.<String, DatasetModule>emptyMap());

    // start
    datasetTypeManager.startAndWait();

    // Test if we load
    Id.DatasetModule extModuleId = Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, FakeExtDatasetModule.NAME);
    DatasetModuleMeta moduleMeta = datasetTypeManager.getModule(extModuleId);

    Assert.assertNotNull(moduleMeta);
    Assert.assertTrue(FakeExtDatasetModule.class.getName().equals(moduleMeta.getClassName()));
  }


  // Helper classes.

  public static class FakeExtDatasetModule implements DatasetModule {

    public static final String NAME = "fakeExtDSModule";

    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = registry.get("keyValueTable");
      registry.add(new FakeExtDatasetDefinition(FakeExtDataset.TYPE_NAME, kvTableDef));
      registry.add(new FakeExtDatasetDefinition(FakeExtDataset.class.getName(), kvTableDef));

      LOG.info("HACK REGISTER FAKE EXT DATASETMODULE IS CALLED.");
    }
  }

  public static class FakeExtDatasetDefinition extends AbstractDatasetDefinition<FakeExtDataset, DatasetAdmin> {

    private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

    public FakeExtDatasetDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueDef) {
      super(name);
      Preconditions.checkArgument(keyValueDef != null, "KeyValueTable definition is required");
      this.tableDef = keyValueDef;
    }

    @Override
    public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
      return DatasetSpecification.builder(instanceName, getName())
        .properties(properties.getProperties())
        .datasets(tableDef.configure("objects", properties))
        .build();
    }

    @Override
    public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                 ClassLoader classLoader) throws IOException {
      return tableDef.getAdmin(datasetContext, spec.getSpecification("objects"), classLoader);
    }

    @Override
    public FakeExtDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                     Map<String, String> arguments, ClassLoader classLoader) throws IOException {
      DatasetSpecification kvTableSpec = spec.getSpecification("objects");
      KeyValueTable table = tableDef.getDataset(datasetContext, kvTableSpec, arguments, classLoader);

      return new FakeExtDataset(spec.getName(), table);
    }
  }


  public static class FakeExtDataset extends AbstractDataset implements BatchReadable<byte[], byte[]>,
    RecordScannable<KeyValue<byte[], byte[]>> {

    public static final String TYPE_NAME = "fakeType";

    private KeyValueTable table;

    public FakeExtDataset(String instanceName, KeyValueTable table) {
      super(instanceName, table);
      this.table = table;
    }

    public byte[] get(byte[] key) {
      return table.read(key);
    }

    public void put(byte[] key, byte[] value) {
      table.write(key, value);
    }

    @Override
    public Type getRecordType() {
      return table.getRecordType();
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public RecordScanner<KeyValue<byte[], byte[]>> createSplitRecordScanner(Split split) {
      return table.createSplitRecordScanner(split);
    }

    @Override
    public SplitReader<byte[], byte[]> createSplitReader(Split split) {
      return table.createSplitReader(split);
    }
  }

}
