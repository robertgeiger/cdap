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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.Id;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Dataset for Completed Workflows
 */
public class WorkflowDataset extends AbstractDataset {

  private static final Gson GSON = new Gson();
  private static final byte[] RUNID = Bytes.toBytes("runId");
  private static final byte[] TIME_TAKEN = Bytes.toBytes("timeTaken");
  private static final byte[] NODES = Bytes.toBytes("nodes");
  private static final String DELIMITER = ":";
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDataset.class);
  private static final Type ACTION_RUNS_TYPE = new TypeToken<List<ActionRuns>>() { }.getType();

  private final Table table;

  @Inject
  public WorkflowDataset(Table table) {
    super("ignored", table);
    this.table = table;
  }

  private byte[] createRowKey(String key, long time) {
    byte[] result = Bytes.toBytes(key);
    byte[] rowKey = new byte[result.length + 8];
    int offset = Bytes.putBytes(rowKey, 0, result, 0, result.length);
    Bytes.putBytes(rowKey, offset, Bytes.toBytes(time), 0, Bytes.SIZEOF_LONG);
    return rowKey;
  }

  public void write(Id.Program id, RunRecordMeta runRecordMeta, List<ActionRuns> actionRunsList) {

    long start = runRecordMeta.getStartTs();
    String key = id.getApplication().getNamespaceId() + DELIMITER
      + id.getApplicationId() + DELIMITER + id.getId() + DELIMITER;
    byte[] rowKey = createRowKey(key, start);

    long timeTaken = runRecordMeta.getStopTs() - start;

    String value = GSON.toJson(actionRunsList, ACTION_RUNS_TYPE);

    table.put(rowKey, RUNID, Bytes.toBytes(runRecordMeta.getPid()));
    table.put(rowKey, TIME_TAKEN, Bytes.toBytes(timeTaken));
    table.put(rowKey, NODES, Bytes.toBytes(value));
  }

  public List<WorkflowRunRecord> scan(Id.Program id, long timeRangeStart, long timeRangeEnd) {
    String namespaceId = id.getApplication().getNamespaceId();
    String applicationId = id.getApplicationId();
    String workflowId = id.getId();

    String key = namespaceId + DELIMITER + applicationId + DELIMITER + workflowId + DELIMITER;
    byte[] startRowKey = createRowKey(key, timeRangeStart);
    byte[] endRowKey = createRowKey(key, timeRangeEnd);
    Scan scan = new Scan(startRowKey, endRowKey);

    Scanner scanner = table.scan(scan);
    Row indexRow;
    List<WorkflowRunRecord> workflowRunRecordList = new ArrayList<>();
    while ((indexRow = scanner.next()) != null) {
      Map<byte[], byte[]> columns = indexRow.getColumns();
      String workflowRunId = Bytes.toString(columns.get(RUNID));
      long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));

      List<ActionRuns> actionRunsList = GSON.fromJson(Bytes.toString(columns.get(NODES)), ACTION_RUNS_TYPE);
      WorkflowRunRecord workflowRunRecord = new WorkflowRunRecord(workflowRunId, timeTaken, actionRunsList);
      workflowRunRecordList.add(workflowRunRecord);
    }
    return workflowRunRecordList;
  }

  /**
   * Internal class to keep track of Workflow Run Records
   */
  public static class WorkflowRunRecord {
    private String workflowRunId;
    private long timeTaken;
    private List<ActionRuns> actionRuns;

    public WorkflowRunRecord(String workflowRunId, long timeTaken, List<ActionRuns> actionRuns) {
      this.actionRuns = actionRuns;
      this.timeTaken = timeTaken;
      this.workflowRunId = workflowRunId;
    }

    public long getTimeTaken() {
      return timeTaken;
    }

    public List<ActionRuns> getActionRuns() {
      return actionRuns;
    }

    public String getWorkflowRunId() {
      return workflowRunId;
    }
  }

  /**
   * Internal Class for keeping track of actions in a workflow
   */
  public static class ActionRuns {
    private String runId;
    private long timeTaken;
    private String name;

    public ActionRuns(String name, String runId, long timeTaken) {
      this.name = name;
      this.runId = runId;
      this.timeTaken = timeTaken;
    }

    public Long getTimeTaken() {
      return timeTaken;
    }

    public String getName() {
      return name;
    }

    public String getRunId() {
      return runId;
    }
  }
}
