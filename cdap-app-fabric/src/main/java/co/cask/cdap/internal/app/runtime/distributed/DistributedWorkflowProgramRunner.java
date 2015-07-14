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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.spark.SparkContextConfig;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * A {@link ProgramRunner} to start a {@link Workflow} program in distributed mode.
 */
public final class DistributedWorkflowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedWorkflowProgramRunner.class);

  @Inject
  public DistributedWorkflowProgramRunner(TwillRunner twillRunner, Configuration hConf, CConfiguration cConf) {
    super(twillRunner, createConfiguration(hConf), cConf);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, File> localizeFiles, ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    // Localize the spark-assembly jar, since workflow can execute spark.
    File sparkAssemblyJar = SparkUtils.locateSparkAssemblyJar();
    localizeFiles.put(sparkAssemblyJar.getName(), sparkAssemblyJar);

    LOG.info("Launching distributed workflow: " + program.getName() + ":" + workflowSpec.getName());
    TwillController controller = launcher.launch(
      new WorkflowTwillApplication(program, workflowSpec, localizeFiles, eventHandler),
      sparkAssemblyJar.getName()
    );
    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new WorkflowTwillProgramController(program.getName(), controller, runId).startListen();
  }

  private static Configuration createConfiguration(Configuration hConf) {
    Configuration configuration = new Configuration(hConf);
    configuration.set(SparkContextConfig.HCONF_ATTR_EXECUTION_MODE, SparkContextConfig.YARN_EXECUTION_MODE);
    return configuration;
  }
}
