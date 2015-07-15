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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDefinition;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetTableMigrator;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.SortedMap;

public class PFSUpgraderTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static DatasetFramework framework;
  private static PFSUpgrader pfsUpgrader;

  private static DatasetProperties dsProps = PartitionedFileSetProperties.builder()
    .setPartitioning(Partitioning.builder().addStringField("league").addIntField("season").build())
    .setBasePath("/temp/fs2")
    .build();

  @BeforeClass
  public static void setup() throws Exception {
    framework = dsFrameworkUtil.getFramework();
    pfsUpgrader = new PFSUpgrader(
      // we can pass null for HBaseAdmin in test case, since we do not test it
      null,
      new PartitionedFileSetTableMigrator(null, null, framework),
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
          return dsFrameworkUtil.newTransactionExecutor(Iterables.toArray(txAwares, TransactionAware.class));
        }
      },
      framework
    );
  }


  @Test
  public void testSimpleUpgrade() throws Exception {
    Id.DatasetInstance ds1 = Id.DatasetInstance.from("test", "fs1");
    framework.addInstance(PartitionedFileSet.class.getName(), ds1,
                          FileSetProperties.builder().setBasePath("/temp/fs1").build());
    // specs created by the current DSFramework already use an IndexedTable for the partitions dataset
    Assert.assertTrue(pfsUpgrader.alreadyUpgraded(framework.getDatasetSpec(ds1)));


    // test conversion from old PartitionedFileSet spec to current spec
        DatasetSpecification oldResultsSpec = constructOldPfsSpec("results", dsProps.getProperties());
    DatasetSpecification newResultsSpec = pfsUpgrader.convertSpec(oldResultsSpec.getName(), oldResultsSpec);

    // the files Dataset shouldn't be changed
    Assert.assertEquals(oldResultsSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME),
                        newResultsSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME));

    DatasetSpecification newPartitionsSpec =
      newResultsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);

    assertIsNewPartitionsTable(newPartitionsSpec);
  }

  @Test
  public void testEmbeddedPfsUpgrade() throws Exception {
    DatasetSpecification pfsSpec = constructOldPfsSpec("results", dsProps.getProperties());

    DatasetSpecification embeddingDsSpec = DatasetSpecification.builder("outerDataset", "customDs")
      .datasets(pfsSpec)
      .build();

    Assert.assertTrue(pfsUpgrader.needsConverting(embeddingDsSpec));

    Map<Id.Namespace, DatasetSpecification> datasetInstances = Maps.newHashMap();
    DatasetSpecification convertedSpec =
      pfsUpgrader.recursivelyMigrateSpec(Constants.DEFAULT_NAMESPACE_ID, embeddingDsSpec.getName(),
                                         embeddingDsSpec, datasetInstances);
    DatasetSpecification migratedEmbeddedPfsSpec = convertedSpec.getSpecification("results");
    DatasetSpecification newPartitionsSpec =
      migratedEmbeddedPfsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    assertIsNewPartitionsTable(newPartitionsSpec);
  }

  private void assertIsNewPartitionsTable(DatasetSpecification dsSpec) {
    // the new partitions should be an IndexedTable and should be set to index appropriately
    Assert.assertEquals(IndexedTable.class.getName(), dsSpec.getType());
    Assert.assertEquals(PartitionedFileSetDefinition.INDEXED_COLS,
                        dsSpec.getProperty(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY));

    // the embedded tables of Indexed Table should be named 'i' and 'd', and be of type Table
    SortedMap<String, DatasetSpecification> indexedTableEmbeddedTables = dsSpec.getSpecifications();
    Assert.assertEquals(ImmutableSet.of("i", "d"), indexedTableEmbeddedTables.keySet());
    for (DatasetSpecification datasetSpecification : indexedTableEmbeddedTables.values()) {
      Assert.assertEquals(Table.class.getName(), datasetSpecification.getType());
    }
  }

  private DatasetSpecification constructOldPfsSpec(String name, Map<String, String> properties) {
    DatasetSpecification.Builder pfsBuilder = DatasetSpecification.builder(name, PartitionedFileSet.class.getName());
    pfsBuilder.properties(properties);

    DatasetSpecification filesSpec =
      DatasetSpecification.builder(PartitionedFileSetDefinition.FILESET_NAME, FileSet.class.getName())
        .properties(properties)
        .build();

    DatasetSpecification partitionsSpec =
      DatasetSpecification.builder(PartitionedFileSetDefinition.PARTITION_TABLE_NAME, Table.class.getName())
        .properties(properties)
        .build();

    pfsBuilder.datasets(filesSpec, partitionsSpec);
    return pfsBuilder.build();
  }

}
