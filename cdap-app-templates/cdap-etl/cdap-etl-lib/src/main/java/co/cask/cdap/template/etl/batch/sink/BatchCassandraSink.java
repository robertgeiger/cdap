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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.hadoop.BulkOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link BatchSink} that writes data to Cassandra.
 * <p/>
 * This {@link BatchCassandraSink} takes a {@link StructuredRecord} in,
 * converts it to columns, and writes it to the Cassandra server.
 * <p/>
 */
@Plugin(type = "sink")
@Name("Cassandra")
@Description("CDAP Cassandra Batch Sink takes the structured record from the input source " +
  "and converts each field to a byte buffer, then puts it in the keyspace and column family specified by the user. " +
  "The Cassandra server should be running prior to creating the adapter.")
public class BatchCassandraSink extends BatchSink<StructuredRecord, Map<String, ByteBuffer>, List<ByteBuffer>> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchCassandraSink.class);

  private final CassandraConfig config;

  public BatchCassandraSink(CassandraConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();

    ConfigHelper.setOutputColumnFamily(conf, config.keyspace, config.columnFamily);
    ConfigHelper.setOutputInitialAddress(conf, config.initialAddress);
    ConfigHelper.setOutputPartitioner(conf, config.partitioner);
    ConfigHelper.setOutputRpcPort(conf, config.port);

    // The query needs to include the non-primaryKey columns. Creating the query
    String query = "UPDATE " + config.keyspace + "." + config.columnFamily + " SET ";
    for (String column : config.columns.split(",")) {
      if (!Arrays.asList(config.primaryKey.split(",")).contains(column)) {
        query += column + " = ?, ";
      }
    }
    query = query.substring(0, query.lastIndexOf(",")) + " "; //to remove the last comma
    CqlConfigHelper.setOutputCql(job.getConfiguration(), query);

    //ideally, we will use CqlBulkOutputFormat once Cassandra patches
    job.setOutputFormatClass(CqlOutputFormat.class);
  }

  @Override
  public void transform(StructuredRecord record,
                        Emitter<KeyValue<Map<String, ByteBuffer>, List<ByteBuffer>>> emitter) throws Exception {
    Map<String, ByteBuffer> keys = new LinkedHashMap<>();
    for (String key : config.primaryKey.split(",")) {
      Preconditions.checkNotNull(record.get(key), "Primary key " + key + " is not present in this record: " +
        StructuredRecordStringConverter.toDelimitedString(record, ";"));
      keys.put(key, encodeObject(record.get(key), record.getSchema().getField(key).getSchema()));
    }
    emitter.emit(new KeyValue<>(keys, getColumns(record)));
  }

  private List<ByteBuffer> getColumns(StructuredRecord record) throws Exception {
    List<ByteBuffer> columns = new ArrayList<>();
    for (String columnName : config.columns.split(",")) {

      //Cassandra allows multiple primary keys, so splitting that list on a comma
      // and checking that the current column isn't a primary key
      if (!Arrays.asList(config.primaryKey.split(",")).contains(columnName)) {
        columns.add(encodeObject(record.get(columnName),
                                 record.getSchema().getField(columnName).getSchema()));
      }
    }
    return columns;
  }

  private ByteBuffer encodeObject(Object object, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      case BOOLEAN:
        byte[] bytes = new byte[1];
        bytes[0] = (byte) ((boolean) object ? 1 : 0);
        return ByteBuffer.wrap(bytes);
      case INT:
        return ByteBufferUtil.bytes((int) object);
      case LONG:
        return ByteBufferUtil.bytes((long) object);
      case FLOAT:
        return ByteBufferUtil.bytes((float) object);
      case DOUBLE:
        return ByteBufferUtil.bytes((double) object);
      case BYTES:
        return ByteBuffer.wrap((byte[]) object);
      case STRING:
      case ENUM:
        // Currently there is no standard container to represent enum type
        return ByteBufferUtil.bytes((String) object);
      case UNION:
        if (schema.isNullableSimple()) {
          return object == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : encodeObject(object, schema.getNonNullable());
        }
    }
    throw new IOException("Unsupported schema: " + schema);
  }

  /**
   * Config class for Batch ElasticsearchSink
   */
  public static class CassandraConfig extends PluginConfig {
    @Name(Properties.Cassandra.PARITIONER)
    private String partitioner;

    @Name(Properties.Cassandra.PORT)
    private String port;

    @Name(Properties.Cassandra.COLUMN_FAMILY)
    private String columnFamily;

    @Name(Properties.Cassandra.KEYSPACE)
    private String keyspace;

    @Name(Properties.Cassandra.INITIAL_ADDRESS)
    private String initialAddress;

    @Name(Properties.Cassandra.COLUMNS)
    private String columns;

    @Name(Properties.Cassandra.PRIMARY_KEY)
    private String primaryKey;

    @Name(Properties.Cassandra.BUFFER_SIZE)
    //int bufferSize;

    public CassandraConfig(String partitioner, String port, String columnFamily, String keyspace,
                           String initialAddress, String columns, String primaryKey) {
      this.partitioner = partitioner;
      this.initialAddress = initialAddress;
      this.port = port;
      this.columnFamily = columnFamily;
      this.keyspace = keyspace;
      this.columns = columns;
      this.primaryKey = primaryKey;
    }
  }
}
