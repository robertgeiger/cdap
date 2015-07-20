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

package co.cask.cdap.template.etl.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.TransformContext;
import co.cask.cdap.template.etl.common.StructuredRecordSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Filters records using custom javascript provided by the config.
 */
@Plugin(type = "transform")
@Name("Validator")
@Description("Executes user provided Javascript in order to transform one record into another")
public class ScriptValidatorTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ScriptValidatorTransform.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final String FUNCTION_NAME = "dont_name_your_function_this";
  private static final String VARIABLE_NAME = "dont_name_your_variable_this";
  private ScriptEngine engine;
  private Invocable invocable;
  private String errorDatasetName;
  private String schema;
  private final Config config;

  /**
   * Configuration for the script transform.
   */
  public static class Config extends PluginConfig {
    @Description("Javascript defining the function to validate input. The script must implement a function " +
      "called 'validate', which take as input a Json object that represents the input record and context, " +
      "and returns a Json object that specifies whether the input is valid or not. " +
      "For example, " +
      "'function validate(input, ctx) {return input.cost < 100 ? { result : true } : { result : false }; }' " +
      "will mark any input whose cost is less than 100 as valid object by returning result=true. Input objects " +
      "whose cost is more than or equal to 100 will be marked as invalid by returning result=false")
    private final String script;

    @Description("Rules to generate the validation script. Note: the plugin does not generate the script!")
    @Nullable
    private final String rules;

    @Description("The Dataset to write the input objects that failed validation.")
    @Nullable
    private final String errorDataset;

    @Description("Input/output schema")
    @Nullable
    private final String schema;

    public Config(String script, String rules, String errorDataset, String schema) {
      this.script = script;
      this.rules = rules;
      this.errorDataset = errorDataset;
      this.schema = schema;
    }
  }

  // for unit tests, otherwise config is injected by plugin framework.
  public ScriptValidatorTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    if (schema != null) {
      pipelineConfigurer.createDataset(errorDatasetName,
                                       Table.class.getName(),
                                       DatasetProperties.builder()
                                         .add(DatasetProperties.SCHEMA, config.schema)
                                         .build()
      );
    } else {
      pipelineConfigurer.createDataset(errorDatasetName, Table.class.getName(),
                                       DatasetProperties.EMPTY);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    ScriptEngineManager manager = new ScriptEngineManager();
    engine = manager.getEngineByName("JavaScript");
    engine.put("_global_ctx", createContext());

    try {
      // this is pretty ugly, but doing this so that we can pass the 'input' json into the validate function.
      // that is, we want people to implement
      // function validate(input) { ... }
      // rather than function validate() { ... } and have them access a global variable in the function
      String script = String.format("function %s() { return validate(%s, _global_ctx); }\n%s",
                                    FUNCTION_NAME, VARIABLE_NAME, config.script);
      engine.eval(script);
    } catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid script.", e);
    }
    invocable = (Invocable) engine;
    if (config.errorDataset == null) {
      throw new IllegalArgumentException("Dataset to write invalid input objects not provided.");
    }
    // Verify if we are able to access errorDatasetName
    errorDatasetName = config.errorDataset;
    ObjectMappedTable<Error> errorTable = getContext().getDataset(errorDatasetName);
    if (errorTable == null) {
      throw new IllegalArgumentException("Cannot access dataset " + errorDatasetName);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    try {
      engine.put("_global_ctx", createContext());
      engine.eval(String.format("var %s = %s;", VARIABLE_NAME, GSON.toJson(input)));
      Map scriptOutput = (Map) invocable.invokeFunction(FUNCTION_NAME);
      boolean result = (boolean) scriptOutput.get("result");

      LOG.info("Got result: {} for input: {}", result, input);
      if (result) {
        LOG.info("Emitting to sink...");
        emitter.emit(input);
      } else {
        LOG.info("Writing to error data...");
        ObjectMappedTable<Error> errorTable = getContext().getDataset(errorDatasetName);
        Error error = new Error(System.currentTimeMillis(), GSON.toJson(input));
        errorTable.write(String.valueOf(System.currentTimeMillis()), error);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not transform input: " + e.getMessage(), e);
    }
  }

  protected ScriptTransformContext createContext() {
    return new ScriptTransformContext();
  }

}
