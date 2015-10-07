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

package co.cask.cdap.stream.store;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.service.MDSStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.namespace.DefaultNamespaceStore;
import co.cask.cdap.namespace.NamespaceStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.TransactionManager;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Test the {@link MDSStreamMetaStore}.
 */
public class MDSStreamMetaStoreTest extends StreamMetaStoreTestBase {

  private static StreamMetaStore streamMetaStore;
  private static DatasetService datasetService;
  private static TransactionManager transactionManager;
  private static NamespaceStore nsStore;

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(CConfiguration.create(), new Configuration()),
      new DataSetServiceModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataFabricModules().getInMemoryModules(),
      new ExploreClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Scopes.SINGLETON);
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          bind(NamespaceStore.class).to(DefaultNamespaceStore.class);
        }
      }
    );

    streamMetaStore = injector.getInstance(StreamMetaStore.class);
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    nsStore = injector.getInstance(NamespaceStore.class);
  }

  @AfterClass
  public static void destroy() throws Exception {
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Override
  protected StreamMetaStore getStreamMetaStore() {
    return streamMetaStore;
  }

  @Override
  protected void createNamespace(String namespace) throws AlreadyExistsException {
    nsStore.create(
      new NamespaceMeta.Builder()
        .setName(namespace)
        .setDescription(namespace)
        .build());
  }

  @Override
  protected void deleteNamespace(String namespaceId) throws NotFoundException {
    nsStore.delete(new Id.Namespace(namespaceId));
  }
}
