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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Upgrading from CDAP version < 3.3 to CDAP version 3.3.
 * <p>
 * This requires updating the TTL property for DatasetSpecification in the DatasetInstanceMDS table.
 */
public class DatasetSpecificationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSpecificationUpgrader.class);
  private static final Gson GSON = new Gson();

  private final HBaseTableUtil tableUtil;
  private final Configuration conf;

  @Inject
  public DatasetSpecificationUpgrader(HBaseTableUtil tableUtil, Configuration conf) {
    this.tableUtil = tableUtil;
    this.conf = conf;
  }

  /**
   * Updates the TTL in the {@link co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS}
   * table for CDAP versions prior to 3.3.
   * <p>
   * The TTL for {@link DatasetSpecification} was stored in milliseconds.
   * Since the spec (as of CDAP version 3.3) is in seconds, the instance MDS entries must be updated.
   * This is to be called only if the current CDAP version is < 3.3.
   * </p>
   * @throws Exception
   */
  public void upgrade() throws Exception {
    TableId datasetSpecId = TableId.from(Id.Namespace.SYSTEM.getId(), DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    if (!tableUtil.tableExists(hBaseAdmin, datasetSpecId)) {
      LOG.info("Dataset instance table does not exist: {}. Should not happen", datasetSpecId);
      return;
    }

    HTable specTable = tableUtil.createHTable(conf, datasetSpecId);

    try {
      ScanBuilder scanBuilder = tableUtil.buildScan();
      scanBuilder.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
      scanBuilder.setMaxVersions();
      try (ResultScanner resultScanner = specTable.getScanner(scanBuilder.build())) {
        Result result;
        while ((result = resultScanner.next()) != null) {
          Put put = new Put(result.getRow());
          for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap :
            result.getMap().entrySet()) {
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnMap : familyMap.getValue().entrySet()) {
              for (Map.Entry<Long, byte[]> columnEntry : columnMap.getValue().entrySet()) {
                Long timeStamp = columnEntry.getKey();
                byte[] colVal = columnEntry.getValue();
                String specEntry = Bytes.toString(colVal);
                DatasetSpecification specification = GSON.fromJson(specEntry, DatasetSpecification.class);
                DatasetSpecification updatedSpec = updateTTLInSpecification(specification);
                colVal = Bytes.toBytes(GSON.toJson(updatedSpec));
                put.add(familyMap.getKey(), columnMap.getKey(), timeStamp, colVal);
              }
            }
          }
          specTable.put(put);
        }
      }
    } finally {
      specTable.flushCommits();
      specTable.close();
    }
  }

  private Map<String, String> updatedProperties(Map<String, String> properties) {
    if (properties.containsKey(Table.PROPERTY_TTL)) {
      SortedMap<String, String> updatedProperties = new TreeMap<>(properties);
      long updatedValue = TimeUnit.MILLISECONDS.toSeconds(Long.valueOf(updatedProperties.get(Table.PROPERTY_TTL)));
      updatedProperties.put(Table.PROPERTY_TTL, String.valueOf(updatedValue));
      return updatedProperties;
    }
    return properties;
  }

  @VisibleForTesting
  DatasetSpecification updateTTLInSpecification(DatasetSpecification specification) {
    Map<String, String> properties = updatedProperties(specification.getProperties());
    List<DatasetSpecification> updatedSpecs = new ArrayList<>();
    for (DatasetSpecification datasetSpecification : specification.getSpecifications().values()) {
      updatedSpecs.add(updateTTLInSpecification(datasetSpecification));
    }
    return DatasetSpecification.builder(specification.getName(),
                                        specification.getType()).properties(properties).datasets(updatedSpecs).build();
  }
}
