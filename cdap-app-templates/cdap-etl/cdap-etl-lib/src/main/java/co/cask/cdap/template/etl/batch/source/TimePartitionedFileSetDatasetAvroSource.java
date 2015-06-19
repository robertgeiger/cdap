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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.common.AvroToStructuredTransformer;
import co.cask.cdap.template.etl.common.ETLUtils;
import co.cask.cdap.template.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

/**
 * A {@link BatchSource} to read Avro record from {@link TimePartitionedFileSet}
 */
@Plugin(type = "source")
@Name("TPFSAvro")
@Description("AVRO Source with Time Partitioned File Dataset")
public class TimePartitionedFileSetDatasetAvroSource extends
  BatchSource<AvroKey<GenericRecord>, NullWritable, StructuredRecord> {

  private static final String SCHEMA_DESC = "The avro schema of the record being written to the Sink as a JSON Object";
  private static final String TPFS_NAME_DESC = "Name of the Time Partitioned FileSet Dataset to which the records " +
    "have to be written. If it doesn't exist, it will be created";
  private static final String BASE_PATH_DESC = "Base path for the time partitioned fileset. Defaults to the " +
    "name of the dataset";
  private static final String DURATION_DESC = "";
  private static final String DELAY_DESC = "";

  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();

  /**
   * Config for TimePartitionedFileSetDatasetAvroSource
   */
  public static class TPFSAvroSourceConfig extends PluginConfig {
    @Description(TPFS_NAME_DESC)
    private String name;

    @Description(SCHEMA_DESC)
    private String schema;

    @Description(BASE_PATH_DESC)
    @Nullable
    private String basePath;

    @Description(DURATION_DESC)
    private String duration;

    @Description(DELAY_DESC)
    @javax.annotation.Nullable
    private String delay;

    private void validate() {
      // check duration and delay
      long durationInMs = ETLUtils.parseDuration(duration);
      Preconditions.checkArgument(durationInMs > 0, "Duration must be greater than 0");
      if (!Strings.isNullOrEmpty(delay)) {
        ETLUtils.parseDuration(delay);
      }
    }
  }

  private final TPFSAvroSourceConfig tpfsAvroSourceConfig;

  public TimePartitionedFileSetDatasetAvroSource(TPFSAvroSourceConfig tpfsAvroSourceConfig) {
    this.tpfsAvroSourceConfig = tpfsAvroSourceConfig;
  }



  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String tpfsName = tpfsAvroSourceConfig.name;
    String basePath = tpfsAvroSourceConfig.basePath == null ? tpfsName : tpfsAvroSourceConfig.basePath;
    tpfsAvroSourceConfig.validate();
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (tpfsAvroSourceConfig.schema))
      .build());
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    long duration = ETLUtils.parseDuration(tpfsAvroSourceConfig.duration);
    long delay = Strings.isNullOrEmpty(tpfsAvroSourceConfig.delay) ? 0 :
      ETLUtils.parseDuration(tpfsAvroSourceConfig.delay);
    long endTime = context.getLogicalStartTime() - delay;
    long startTime = endTime - duration;
    Map<String, String> sourceArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(sourceArgs, startTime);
    TimePartitionedFileSetArguments.setInputEndTime(sourceArgs, endTime);
    TimePartitionedFileSet source = context.getDataset(tpfsAvroSourceConfig.name, sourceArgs);
    context.setInput(tpfsAvroSourceConfig.name, source);
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(tpfsAvroSourceConfig.schema);
    Job job = context.getHadoopJob();
    AvroJob.setInputKeySchema(job, avroSchema);
  }

  @Override
  public void transform(KeyValue<AvroKey<GenericRecord>, NullWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    System.err.println("DEBUGGING - " + input.getKey().datum().getSchema().toString());
    emitter.emit(recordTransformer.transform(input.getKey().datum()));
  }



}