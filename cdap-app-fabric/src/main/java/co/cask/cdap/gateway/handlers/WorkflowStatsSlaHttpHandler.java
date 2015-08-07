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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.app.store.WorkflowDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 *
 */

@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowStatsSlaHttpHandler extends AbstractHttpHandler {
  /**
   * Store manages non-runtime lifecycle.
   */
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowStatsSlaHttpHandler.class);
  protected final WorkflowDataset workflowDataset;
  protected final Store store;
  // protected final Table table;
  private final MetricStore metricStore;

  /**
   * Runtime program service for running and managing programs.
   */

  @Inject
  public WorkflowStatsSlaHttpHandler(MetricStore metricStore,
                                Store store, WorkflowDataset workflowDataset) {
    this.metricStore = metricStore;
    this.store = store;
    this.workflowDataset = workflowDataset;
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/stats")
  public void workflowStats(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("workflow-id") String workflowId,
                            @QueryParam("start") long start,
                            @QueryParam("end") long end) throws Exception {

    int limit = Integer.MAX_VALUE;
    start = 0;
    end = Long.MAX_VALUE;
    List<RunRecordMeta> runRecordMetas;

    Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowId);
    try {
      try {
        // change this
        runRecordMetas = store.getRuns(programId, ProgramRunStatus.COMPLETED, start, end, limit);
      } catch (IllegalArgumentException exception) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Bad request");
        return;
      }
    } catch (SecurityException exception) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "You're not allowed to view this");
      return;
    }

    long avgTimeToComplete = 0;
    int count = 0;
    Map<ProgramRunStatus, Integer> statusCount = Maps.newHashMap();
    for (ProgramRunStatus value : ProgramRunStatus.values()) {
      statusCount.put(value, 0);
    }

    List<RunRecord> filteredRunRecords = Lists.newArrayList();
    for (RunRecord runRecord : runRecordMetas) {
      if (runRecord.getStatus().equals(ProgramRunStatus.COMPLETED)) {
        avgTimeToComplete += runRecord.getStopTs() - runRecord.getStartTs();
        count++;
        filteredRunRecords.add(runRecord);
      }
      statusCount.put(runRecord.getStatus(), statusCount.get(runRecord.getStatus()) + 1);
    }

    avgTimeToComplete = avgTimeToComplete / count;
    Collections.sort(filteredRunRecords, new Comparator<RunRecord>() {
      @Override
      public int compare(RunRecord o1, RunRecord o2) {
        if ((o1.getStopTs() - o1.getStartTs()) > (o2.getStopTs() - o2.getStartTs())) {
          return 1;
        } else if ((o1.getStopTs() - o1.getStartTs()) < (o2.getStopTs() - o2.getStartTs())) {
          return -1;
        } else {
          return 0;
        }
      }
    });

    List<String> slowest10PercentileRuns = Lists.newArrayList();
    int percentile90 = (int) (count * .9);
    for (int i = percentile90; i < count; i++) {
      slowest10PercentileRuns.add(filteredRunRecords.get(i).getPid());
    }

    // Binary search to find the set of runs that are above/below average
    int startIndex = 0, endIndex = filteredRunRecords.size() - 1, midIndex = (startIndex + endIndex) / 2;
    while (startIndex < endIndex) {
      midIndex = (startIndex + endIndex) / 2;
      RunRecord midRunRecord = filteredRunRecords.get(midIndex);
      if (avgTimeToComplete > (midRunRecord.getStopTs() - midRunRecord.getStartTs())) {
        startIndex = midIndex;
        continue;
      } else if (avgTimeToComplete < (midRunRecord.getStopTs() - midRunRecord.getStartTs())) {
        endIndex = midIndex;
        continue;
      } else {
        break;
      }
    }
    int countBelowAverage = midIndex, countAboveAverage = filteredRunRecords.size() - midIndex;
    // Return countBelowAverage, countAboveAverage, slowest10PercentileRuns, avgTime, count
    Map<String, Object> response = Maps.newHashMap();
    response.put("count", count);
    response.put("avg.time.to.complete", avgTimeToComplete);
    response.put("slowest.ten.percentile", slowest10PercentileRuns);
    response.put("count.below.avg", countBelowAverage);
    response.put("count.above.avg", countAboveAverage);
    responder.sendJson(HttpResponseStatus.OK, response);
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/stats")
  public void workflowSingleRunStats(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("workflow-id") String workflowId,
                                     @PathParam("run-id") String runId,
                                     @QueryParam("start") long start,
                                     @QueryParam("end") long end,
                                     @QueryParam("limit") long limit) {
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/run/{run-id}/compare")
  public void comparer(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-id") String appId,
                       @PathParam("workflow-id") String workflowId,
                       @PathParam("run-id") String runId,
                       @QueryParam("other-run") String otherRun
                       ) {

  }
}
