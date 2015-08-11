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
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.internal.app.store.DefaultStore;
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
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
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
  protected final Store store;
  // protected final Table table;
  private final MetricStore metricStore;

  /**
   * Runtime program service for running and managing programs.
   */

  @Inject
  public WorkflowStatsSlaHttpHandler(MetricStore metricStore,
                                Store store) {
    this.metricStore = metricStore;
    this.store = store;
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/stats")
  public void workflowStats(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("workflow-id") String workflowId,
                            @QueryParam("start") String start,
                            @QueryParam("end") String end) throws Exception {

    int limit = Integer.MAX_VALUE;
    long startTime = TimeMathParser.parseTimeInSeconds(start);
    long endTime = TimeMathParser.parseTimeInSeconds(end);

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

    responder.sendJson(HttpResponseStatus.OK, workflowRunRecords);
    return;

    int count = workflowRunRecords.size();

    if (count == 0) {
      responder.sendJson(HttpResponseStatus.OK, "There were no completed runs for this workflow.");
    }

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

    List<String> slowest10PercentileRuns = Lists.newArrayList();
    int percentile90 = (int) (count * .9);
    for (int i = percentile90; i < count; i++) {
      slowest10PercentileRuns.add(workflowRunRecords.get(i).getWorkflowRunId());
    }

    // Return countBelowAverage, countAboveAverage, slowest10PercentileRuns, avgTime, count
    Map<String, Object> response = Maps.newHashMap();
    response.put("count", count);
    response.put("avg.time.to.complete", avgTimeToComplete);
    response.put("slowest.ten.percentile", slowest10PercentileRuns);
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
