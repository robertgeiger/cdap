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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.internal.app.store.WorkflowDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Workflow Statistics Handler
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowStatsSLAHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowStatsSLAHttpHandler.class);
  private final Store store;
  private final MRJobInfoFetcher mrJobInfoFetcher;
  private final MetricStore metricStore;

  @Inject
  WorkflowStatsSLAHttpHandler(Store store, MRJobInfoFetcher mrJobInfoFetcher, MetricStore metricStore) {
    this.store = store;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
    this.metricStore = metricStore;
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/statistics")
  public void workflowStats(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("workflow-id") String workflowId,
                            @QueryParam("start") String start,
                            @QueryParam("end") String end,
                            @QueryParam("percentile") List<Double> percentiles) throws Exception {
    long startTime = TimeMathParser.parseTimeInSeconds(start);
    long endTime = TimeMathParser.parseTimeInSeconds(end);

    if (startTime < 0) {
      throw new BadRequestException("Invalid start time. The time you entered was : " + startTime);
    } else if (endTime < 0) {
      throw new BadRequestException("Invalid end time. The time you entered was : " + endTime);
    } else if (endTime < startTime) {
      throw new BadRequestException("Start time : " + startTime + " cannot be larger than end time : " + endTime);
    }

    for (double i : percentiles) {
      if (i < 0.0 || i > 100.0) {
        throw new BadRequestException("Percentile values have to be greater than or equal to 0 and" +
                                        " less than or equal to 100. Invalid input was " + Double.toString(i));
      }
    }

    Id.Workflow workflow = Id.Workflow.from(Id.Namespace.from(namespaceId), appId, workflowId);
    WorkflowStatistics workflowStatistics = store.getWorkflowStatistics(workflow, startTime, endTime, percentiles);

    if (workflowStatistics == null) {
      responder.sendString(HttpResponseStatus.OK, "There are no statistics associated with this workflow : "
        + workflowId + " in the specified time range.");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, workflowStatistics);
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/statistics")
  public void workflowRunDetail(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("workflow-id") String workflowId,
                                @PathParam("run-id") String runId,
                                @QueryParam("count") int count,
                                @QueryParam("interval") String interval) throws Exception {
    long timeInterval = TimeMathParser.resolutionInSeconds(interval);
    Id.Workflow workflow = Id.Workflow.from(Id.Namespace.from(namespaceId), appId, workflowId);
    Set<WorkflowDataset.WorkflowRunRecord> workflowRunRecords =
      store.retrieveSpacedRecords(workflow, runId, count, timeInterval);
    Map<String, WorkflowStatistics.DetailedStatistics> detailedStatisticsMap = new HashMap<>();
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      WorkflowStatistics.DetailedStatistics detailedStatistics =
        getDetailedRecord(workflow, workflowRunRecord.getWorkflowRunId());
      detailedStatisticsMap.put(workflowRunRecord.getWorkflowRunId(), detailedStatistics);
    }
    Map<String, Map<String, Object>> formattedStatisticsMap = convert(detailedStatisticsMap);

    responder.sendJson(HttpResponseStatus.OK, formattedStatisticsMap);
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/compare/run-id/{run-id}")
  public void compare(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("app-id") String appId,
                      @PathParam("workflow-id") String workflowId,
                      @PathParam("run-id") String runId,
                      @QueryParam("other-run-id") String otherRunId) throws Exception {
    Id.Workflow id = Id.Workflow.from(Id.Namespace.from(namespaceId), appId, workflowId);
    WorkflowStatistics.DetailedStatistics detailedStatistics = getDetailedRecord(id, runId);
    WorkflowStatistics.DetailedStatistics otherDetailedStatistics = getDetailedRecord(id, otherRunId);
    if (detailedStatistics == null) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "The run-id you provided was not correct.");
      return;
    }
    if (otherDetailedStatistics == null) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "The other run-id you provided was not correct.");
      return;
    }
    // TODO: Change format of return such that the results are intertwined.
    Map<String, WorkflowStatistics.DetailedStatistics> detailedStatisticsMap = new HashMap<>();
    detailedStatisticsMap.put(runId, detailedStatistics);
    detailedStatisticsMap.put(otherRunId, otherDetailedStatistics);

    Map<String, Map<String, Object>> formattedStatisticsMap = convert(detailedStatisticsMap);
    responder.sendJson(HttpResponseStatus.OK, formattedStatisticsMap);
  }

  private Map<String, Map<String, Object>> convert(Map<String, WorkflowStatistics.DetailedStatistics>
                                                                        workflowDetailedStatisticsMap) {
    Map<String, Map<String, Object>> formattedStatisticsMap = new HashMap<>();
    for (Map.Entry<String, WorkflowStatistics.DetailedStatistics> entry : workflowDetailedStatisticsMap.entrySet()) {
      Map<String, Object> internalMap = entry.getValue().getProgramToStatistics();
      for (Map.Entry<String, Object> internalEntry : internalMap.entrySet()) {
        Map<String, Object> newMap;
        if ((newMap = formattedStatisticsMap.get(internalEntry.getKey())) == null) {
          newMap = new HashMap<>();
          formattedStatisticsMap.put(internalEntry.getKey(), newMap);
        }
        newMap.put(entry.getKey(), internalEntry.getValue());
      }
    }
    return formattedStatisticsMap;
  }

  @Nullable
  public WorkflowStatistics.DetailedStatistics getDetailedRecord(Id.Workflow workflowId,
                                                                 String runId) throws Exception {
    WorkflowDataset.WorkflowRunRecord workflowRunRecord = store.getWorkflowRun(workflowId, runId);
    if (workflowRunRecord == null) {
      return null;
    }
    List<WorkflowDataset.ProgramRun> programRuns = workflowRunRecord.getProgramRuns();
    Map<String, Object> details = new HashMap<>();
    for (WorkflowDataset.ProgramRun actionRun : programRuns) {
      if (actionRun.getProgramType() == ProgramType.MAPREDUCE) {
        details.put(actionRun.getName(),
                    getMapreduceDetails(Id.Program.from(workflowId.getNamespaceId(), workflowId.getApplicationId(),
                                                        actionRun.getProgramType(), actionRun.getName()),
                                        actionRun.getRunId()));
      } else if (actionRun.getProgramType() == ProgramType.SPARK) {
        details.put(actionRun.getName(),
                    getSparkDetails(Id.Program.from(workflowId.getNamespaceId(), workflowId.getApplicationId(),
                                                    actionRun.getProgramType(), actionRun.getName()),
                                    actionRun.getRunId()));
      }
    }
    return new WorkflowStatistics.DetailedStatistics(details);
  }

  private MRJobInfo getMapreduceDetails(Id.Program mapreduceProgram, String runId) throws Exception {
    Id.Run mrRun = new Id.Run(mapreduceProgram, runId);
    return mrJobInfoFetcher.getMRJobInfo(mrRun);
  }

  private Map<String, List<TimeValue>> getSparkDetails(Id.Program sparkProgram, String runId) throws Exception {
    System.out.println("Trigger : " + runId);
    Map<String, String> context = new HashMap<>();
    context.put(Constants.Metrics.Tag.NAMESPACE, sparkProgram.getNamespaceId());
    context.put(Constants.Metrics.Tag.APP, sparkProgram.getApplicationId());
    context.put(Constants.Metrics.Tag.SPARK, sparkProgram.getId());
    context.put(Constants.Metrics.Tag.RUN_ID, runId);

    List<TagValue> tags = new ArrayList<>();
    tags.add(new TagValue(Constants.Metrics.Tag.NAMESPACE, sparkProgram.getNamespaceId()));
    tags.add(new TagValue(Constants.Metrics.Tag.APP, sparkProgram.getApplicationId()));
    tags.add(new TagValue(Constants.Metrics.Tag.SPARK, sparkProgram.getId()));
    tags.add(new TagValue(Constants.Metrics.Tag.RUN_ID, runId));
    MetricSearchQuery metricSearchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE, -1, tags);
    Collection<String> metricNames = metricStore.findMetricNames(metricSearchQuery);
    Map<String, List<TimeValue>> overallResult = new HashMap<>();
    String name;
    List<TimeValue> timeValues;
    for (String metricName : metricNames) {
      Collection<MetricTimeSeries> resultPerQuery = metricStore.query(
        new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                            context, new ArrayList<String>()));

      for (MetricTimeSeries metricTimeSeries : resultPerQuery) {
        name = metricTimeSeries.getMetricName();
        timeValues = metricTimeSeries.getTimeValues();
        overallResult.put(name, timeValues);
      }
    }
    return overallResult;
  }
}
