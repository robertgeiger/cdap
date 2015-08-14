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

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.internal.app.store.WorkflowDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Workflow Statistics Handler
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowStatsSlaHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowStatsSlaHttpHandler.class);
  protected final Store store;
  private final MetricStore metricStore;

  @Inject
  public WorkflowStatsSlaHttpHandler(MetricStore metricStore, Store store) {
    this.metricStore = metricStore;
    this.store = store;
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

    if (endTime < startTime || startTime < 0 || endTime < 0) {
      throw new BadRequestException("Wrong start or end time.");
    }

    for (double i : percentiles) {
      if (i < 0 || i > 100) {
        throw new BadRequestException("Percentile values have to be greater than 0 and less than 100");
      }
    }

    List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords;

    Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowId);
    try {
      try {
        workflowRunRecords = store.getWorkflowRuns(programId, startTime, endTime);

      } catch (IllegalArgumentException exception) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Bad request");
        return;
      }
    } catch (SecurityException exception) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "You're not allowed to view this");
      return;
    }

    int count = workflowRunRecords.size();

    if (count == 0) {
      responder.sendJson(HttpResponseStatus.OK, "There were no completed runs for this workflow.");
      return;
    }

    long average = 0;
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord: workflowRunRecords) {
      average += workflowRunRecord.getTimeTaken();
    }
    average /= count;

    workflowRunRecords = sort(workflowRunRecords);

    Map<String, Long> percentileToTime = Maps.newHashMap();
    Map<String, List<String>> percentileToRunids = Maps.newHashMap();
    for (double i : percentiles) {
      List<String> percentileRun = new ArrayList();
      int percentileStart = (int) ((i * count) / 100);
      for (int j = percentileStart; j < count; j++) {
        percentileRun.add(workflowRunRecords.get(j).getWorkflowRunId());
      }
      percentileToRunids.put(Double.toString(i), percentileRun);
      percentileToTime.put(Double.toString(i), workflowRunRecords.get(percentileStart).getTimeTaken());
    }

    Map<String, List<Long>> actionToRunRecord = getActionRuns(workflowRunRecords);

    Map<String, Map<String, Long>> actionToPercentile = Maps.newHashMap();
    for (Map.Entry<String, List<Long>> entry : actionToRunRecord.entrySet()) {
      long mean = 0;
      for (long iterator: entry.getValue()) {
        mean += iterator;
      }
      mean /= entry.getValue().size();
      Map temp = Maps.newHashMap();
      temp.put("mean", mean);
      actionToPercentile.put(entry.getKey(), temp);
    }

    for (Map.Entry<String, List<Long>> entry : actionToRunRecord.entrySet()) {
      List<Long> runList = entry.getValue();
      Collections.sort(runList);
      for (double percentile : percentiles) {
        long percentileValue = runList.get((int) ((percentile * runList.size()) / 100));
        actionToPercentile.get(entry.getKey()).put(Double.toString(percentile), percentileValue);
      }
    }
    
    BasicStatistics basicStatistics = new BasicStatistics(startTime, endTime, count, average, percentileToTime,
                                                          percentileToRunids, actionToPercentile);

    responder.sendJson(HttpResponseStatus.OK, basicStatistics);
  }

  private List<WorkflowDataset.WorkflowRunRecord> sort(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Collections.sort(workflowRunRecords, new Comparator<WorkflowDataset.WorkflowRunRecord>() {
      @Override
      public int compare(WorkflowDataset.WorkflowRunRecord o1, WorkflowDataset.WorkflowRunRecord o2) {
        if (o1.getTimeTaken() > o2.getTimeTaken()) {
          return 1;
        } else if (o1.getTimeTaken() < o2.getTimeTaken()) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    return workflowRunRecords;
  }

  private Map<String, List<Long>> getActionRuns(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Map<String, List<Long>> actionToRunRecord = Maps.newHashMap();
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord: workflowRunRecords) {
      for (WorkflowDataset.ActionRuns runs: workflowRunRecord.getActionRuns()) {
        List<Long> runList;
        if (actionToRunRecord.get(runs.getName()) == null) {
          runList = Lists.newArrayList();
        } else {
          runList = actionToRunRecord.get(runs.getName());
        }
        runList.add(runs.getTimeTaken());
        actionToRunRecord.put(runs.getName(), runList);
      }
    }
    return actionToRunRecord;
  }

  @VisibleForTesting
  public static class BasicStatistics {
    private long startTime;
    private long endTime;
    private int runs;
    private double avgRunTime;
    private Map<String, Long> percentileToTime;
    private Map<String, List<String>> percentileToRunids;
    private Map<String, Map<String, Long>> actionToPercentile;

    public BasicStatistics(long startTime, long endTime, int runs, double avgRunTime,
                           Map<String, Long> percentileToTime,
                           Map<String, List<String>> percentileToRunids,
                           Map<String, Map<String, Long>> actionToPercentile) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.runs = runs;
      this.avgRunTime = avgRunTime;
      this.percentileToTime = percentileToTime;
      this.percentileToRunids = percentileToRunids;
      this.actionToPercentile = actionToPercentile;
    }

    public int getRuns() {
      return runs;
    }

    public double getAvgRunTime() {
      return avgRunTime;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    public Map<String, Long> getPercentileToTime() {
      return percentileToTime;
    }

    public Map<String, List<String>> getPercentileToRunids() {
      return percentileToRunids;
    }

    public Map<String, Map<String, Long>> getActionToPercentile() {
      return actionToPercentile;
    }
  }
}
