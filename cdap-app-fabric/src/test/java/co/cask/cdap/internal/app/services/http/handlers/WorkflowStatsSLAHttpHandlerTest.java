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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.WorkflowApp;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.PercentileInformation;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.WorkflowStatsComparison;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler}
 */
public class WorkflowStatsSLAHttpHandlerTest extends AppFabricTestBase {

  @Test
  public void testStatistics() throws Exception {

    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    Id.Program workflowProgram =
      Id.Workflow.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.WORKFLOW, workflowName);
    Id.Program mapreduceProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.MAPREDUCE, mapreduceName);
    Id.Program sparkProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.SPARK, sparkName);

    long startTime = System.currentTimeMillis();
    long currentTimeMillis = startTime;
    String outlierRunId = null;
    for (int i = 0; i < 10; i++) {
      // work-flow runs every 5 minutes
      currentTimeMillis = startTime + (i * TimeUnit.MINUTES.toMillis(5));
      RunId workflowRunId = RunIds.generate(currentTimeMillis);
      store.setStart(workflowProgram, workflowRunId.getId(), RunIds.getTime(workflowRunId, TimeUnit.SECONDS));

      // MR job starts 2 seconds after workflow started
      RunId mapreduceRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(2));
      store.setWorkflowProgramStart(mapreduceProgram, mapreduceRunid.getId(), workflowProgram.getId(),
                                    workflowRunId.getId(), mapreduceProgram.getId(),
                                    RunIds.getTime(mapreduceRunid, TimeUnit.SECONDS), null, null);
      store.setStop(mapreduceProgram, mapreduceRunid.getId(),
                    // map-reduce job ran for 17 seconds
                    TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 19,
                    ProgramRunStatus.COMPLETED);

      // This makes sure that not all runs have Spark programs in them
      if (i < 5) {
        // spark starts 20 seconds after workflow starts
        RunId sparkRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(20));
        store.setWorkflowProgramStart(sparkProgram, sparkRunid.getId(), workflowProgram.getId(),
                                      workflowRunId.getId(), sparkProgram.getId(),
                                      RunIds.getTime(sparkRunid, TimeUnit.SECONDS), null, null);
        // spark job runs for 38 seconds
        long stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 58;
        if (i == 4) {
          // spark job ran for 100 seconds. 62 seconds greater than avg.
          stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 120;
        }
        store.setStop(sparkProgram, sparkRunid.getId(), stopTime, ProgramRunStatus.COMPLETED);
      }

      // workflow ran for 1 minute
      long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 60;
      if (i == 4) {
        // spark job ran longer for this run
        workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 122;
        outlierRunId = workflowRunId.getId();
      }

      store.setStop(workflowProgram, workflowRunId.getId(), workflowStopTime, ProgramRunStatus.COMPLETED);
    }

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                                     "&percentile=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(startTime),
                                   TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + TimeUnit.MINUTES.toSeconds(2),
                                   "99");

    HttpResponse response = doGet(request);
    WorkflowStatistics workflowStatistics =
      readResponse(response, new TypeToken<WorkflowStatistics>() { }.getType());
    PercentileInformation percentileInformation = workflowStatistics.getPercentileInformationList().get(0);
    Assert.assertEquals(1, percentileInformation.getRunIdsOverPercentile().size());
    Assert.assertEquals(outlierRunId, percentileInformation.getRunIdsOverPercentile().get(0));
    Assert.assertEquals("5", workflowStatistics.getNodes().get(sparkName).get("runs"));

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                            WorkflowApp.class.getSimpleName(), workflowProgram.getId(), "now", "0", "90", "95");

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(),
                        response.getStatusLine().getStatusCode());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                            WorkflowApp.class.getSimpleName(), workflowProgram.getId(), "now", "0", "90.0", "950");

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(),
                        response.getStatusLine().getStatusCode());
  }

  @Test
  public void testDetails() throws Exception {
    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    Id.Program workflowProgram =
      Id.Workflow.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.WORKFLOW, workflowName);
    Id.Program mapreduceProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.MAPREDUCE, mapreduceName);
    Id.Program sparkProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.SPARK, sparkName);

    List<RunId> runIdList = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      runIdList.add(setupRuns(workflowProgram, mapreduceProgram, sparkProgram, store));
    }

    System.out.println("List : " + runIdList);

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/statistics?limit=%s&interval=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getId(), runIdList.get(6).getId(),
                                   "3", "2s");

    HttpResponse response = doGet(request);
    WorkflowStatsComparison workflowStatistics =
      readResponse(response, new TypeToken<WorkflowStatsComparison>() { }.getType());

    for (WorkflowStatsComparison.ProgramNodes.WorkflowDetails temp : workflowStatistics.getProgramNodesList().iterator()
      .next().getWorkflowDetailsList()) {
      System.out.println("Final : " + temp.getWorkflowRunId());
    }
    // Assert.assertEquals(7, workflowStatistics.getProgramNodesList().iterator().next().getWorkflowDetailsList().size());
  }

  @Test
  public void testCompare() throws Exception {
    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    Id.Program workflowProgram =
      Id.Workflow.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.WORKFLOW, workflowName);
    Id.Program mapreduceProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.MAPREDUCE, mapreduceName);
    Id.Program sparkProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.SPARK, sparkName);

    RunId workflowRun1 = setupRuns(workflowProgram, mapreduceProgram, sparkProgram, store);
    RunId workflowRun2 = setupRuns(workflowProgram, mapreduceProgram, sparkProgram, store);

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/run-id/%s/compare?other-run-id=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getId(), workflowRun1.toString(),
                                   workflowRun2.toString());

    HttpResponse response = doGet(request);
    Collection<WorkflowStatsComparison.ProgramNodes> workflowStatistics =
      readResponse(response, new TypeToken<Collection<WorkflowStatsComparison.ProgramNodes>>() {
      }.getType());

    Assert.assertNotNull(workflowStatistics.iterator().next());
    Assert.assertEquals(2, workflowStatistics.size());

    for (WorkflowStatsComparison.ProgramNodes node : workflowStatistics) {
      if (node.getProgramType() == ProgramType.MAPREDUCE) {
        Assert.assertEquals(38L, (long) node.getWorkflowDetailsList().get(0)
          .getMetrics().get(TaskCounter.MAP_INPUT_RECORDS.name()));
      }
    }
  }

  private RunId setupRuns(Id.Program workflowProgram, Id.Program mapreduceProgram,
                          Id.Program sparkProgram, Store store) throws Exception {
    RunId workflowRunId = RunIds.generate();
    store.setStart(workflowProgram, workflowRunId.getId(), RunIds.getTime(workflowRunId, TimeUnit.SECONDS));

    RunId mapreduceRunid = RunIds.generate();
    store.setWorkflowProgramStart(mapreduceProgram, mapreduceRunid.getId(), workflowProgram.getId(),
                                  workflowRunId.getId(), mapreduceProgram.getId(),
                                  RunIds.getTime(mapreduceRunid, TimeUnit.SECONDS), null, null);
    store.setStop(mapreduceProgram, mapreduceRunid.getId(),
                  TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED);

    Map<String, String> mapTypeContext = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE,
                                                         mapreduceProgram.getNamespaceId(),
                                                         Constants.Metrics.Tag.APP, mapreduceProgram.getApplicationId(),
                                                         Constants.Metrics.Tag.MAPREDUCE, mapreduceProgram.getId(),
                                                         Constants.Metrics.Tag.RUN_ID, mapreduceRunid.toString(),
                                                         Constants.Metrics.Tag.MR_TASK_TYPE,
                                                         MapReduceMetrics.TaskType.Mapper.getId());

    metricStore.add(new MetricValues(mapTypeContext, MapReduceMetrics.METRIC_INPUT_RECORDS, 10, 38L,
                                     MetricType.GAUGE));
    RunId sparkRunid = RunIds.generate();
    store.setWorkflowProgramStart(sparkProgram, sparkRunid.getId(), workflowProgram.getId(),
                                  workflowRunId.getId(), sparkProgram.getId(),
                                  RunIds.getTime(sparkRunid, TimeUnit.SECONDS), null, null);
    store.setStop(sparkProgram, sparkRunid.getId(),
                  TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED);

    store.setStop(workflowProgram, workflowRunId.getId(),
                  TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED);

    // Sleep so that 2 runs of the workflow dont have same start time which causes exception to be thrown
    TimeUnit.SECONDS.sleep(1);
    return workflowRunId;
  }
}
