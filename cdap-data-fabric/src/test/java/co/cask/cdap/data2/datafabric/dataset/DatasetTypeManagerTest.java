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

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.runtime.TransactionInMemoryModule;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Services;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.Map;

/**
 * Testing DatasetTypeManager class.
 */
public class DatasetTypeManagerTest {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeManagerTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private DatasetTypeManager datasetTypeManager;
  private LocationFactory locationFactory;
  private CConfiguration cConf;
  protected TransactionManager txManager;
  private MDSDatasetsRegistry mdsDatasetsRegistry;

  @Before
  public void before() throws Exception {
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
    cConf.set(DatasetTypeManager.CDAP_DATASET_EXT_NAMES, FakeExtDatasetModule.NAME);
    cConf.set(DatasetTypeManager.CDAP_DATASET_EXT_MODULES, FakeExtDatasetModule.class.getName());

    // Tx Manager to support working with datasets
    Configuration txConf = HBaseConfiguration.create();
    CConfigurationUtil.copyTxProperties(cConf, txConf);
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    final Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule());

    DatasetDefinitionRegistryFactory registryFactory = new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
        injector.injectMembers(registry);
        return registry;
      }
    };

    locationFactory = injector.getInstance(LocationFactory.class);
    ImmutableMap<String, DatasetModule> modules = ImmutableMap.<String, DatasetModule>builder()
      .putAll(injector.getInstance(Key.get(new TypeLiteral<Map<String, DatasetModule>>() {
                                           },
                                           Names.named("defaultDatasetModules"))))
      .putAll(DatasetMetaTableUtil.getModules())
      .build();

    mdsDatasetsRegistry =
      new MDSDatasetsRegistry(txSystemClient, new InMemoryDatasetFramework(registryFactory, modules, cConf));
  }

  @After
  public void after()throws Exception {
    Services.chainStop(datasetTypeManager, txManager);
    Locations.deleteQuietly(locationFactory.create(Constants.DEFAULT_NAMESPACE));
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

  public static class FakeExtDatasetModule implements DatasetModule {

    public static final String NAME = "fakeExtDSModule";

    @Override
    public void register(DatasetDefinitionRegistry registry) {
      LOG.info("HACK REGISTER FAKE EXT DATASETMODULE IS CALLED.");
    }
  }
}
