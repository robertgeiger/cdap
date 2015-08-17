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
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.istack.Nullable;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dataset for Completed Workflows and their associated actions
 */
public class WorkflowDataset extends AbstractDataset {

  private static final Gson GSON = new Gson();
  private static final byte[] RUNID = Bytes.toBytes("r");
  private static final byte[] TIME_TAKEN = Bytes.toBytes("t");
  private static final byte[] NODES = Bytes.toBytes("n");
  private static final String DELIMITER = ":";
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDataset.class);
  private static final Type ACTION_RUNS_TYPE = new TypeToken<List<ActionRuns>>() { }.getType();

  private final MRJobInfoFetcher mrJobInfoFetcher;
  private final Table table;

  WorkflowDataset(Table table, MRJobInfoFetcher mrJobInfoFetcher) {
    super("ignored", table);
    this.table = table;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
  }

  private byte[] createRowKey(String key, long time) {
    byte[] result = Bytes.toBytes(key);
    // Size of long is being added to the row key since the start time is being added at the end
    byte[] rowKey = new byte[result.length + Bytes.SIZEOF_LONG];
    int offset = Bytes.putBytes(rowKey, 0, result, 0, result.length);
    Bytes.putBytes(rowKey, offset, Bytes.toBytes(time), 0, Bytes.SIZEOF_LONG);
    return rowKey;
  }

  public void write(Id.Workflow id, RunRecordMeta runRecordMeta, List<ActionRuns> actionRunsList) {
    Joiner joiner = Joiner.on(DELIMITER);
    String key = joiner.join(id.getApplication().getNamespaceId(), id.getApplicationId(), id.getId()) + DELIMITER;
    long start = runRecordMeta.getStartTs();
    byte[] rowKey = createRowKey(key, start);

    long timeTaken = runRecordMeta.getStopTs() - start;

    String value = GSON.toJson(actionRunsList, ACTION_RUNS_TYPE);

    table.put(rowKey, RUNID, Bytes.toBytes(runRecordMeta.getPid()));
    table.put(rowKey, TIME_TAKEN, Bytes.toBytes(timeTaken));
    table.put(rowKey, NODES, Bytes.toBytes(value));
  }

  public List<WorkflowRunRecord> scan(Id.Workflow id, long timeRangeStart, long timeRangeEnd) throws Exception {
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

  @Nullable
  public WorkflowDataset.BasicStatistics getStatistics(Id.Workflow id, long startTime,
                                                    long endTime, List<Double> percentiles) throws Exception {
    List<WorkflowRunRecord> workflowRunRecords = scan(id, startTime, endTime);
    int count = workflowRunRecords.size();

    if (count == 0) {
      return null;
    }

    double avgRunTime = 0.0;
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      avgRunTime += workflowRunRecord.getTimeTaken();
    }
    avgRunTime /= count;

    workflowRunRecords = sort(workflowRunRecords);

    Pair<Map<String, Long>, Map<String, List<String>>> percentilesAndPercentileList =
      getPercentiles(workflowRunRecords, percentiles);

    Map<String, Long> percentileToTime = percentilesAndPercentileList.getFirst();
    Map<String, List<String>> percentileToRunids = percentilesAndPercentileList.getSecond();

    Map<String, List<Long>> actionToRunRecord = getActionRuns(workflowRunRecords);

    Map<String, Map<String, Double>> actionToStatistic = new HashMap<>();
    for (Map.Entry<String, List<Long>> entry : actionToRunRecord.entrySet()) {
      double avgForAction = 0;
      for (long value : entry.getValue()) {
        avgForAction += value;
      }
      avgForAction /= entry.getValue().size();
      Map temp = new HashMap<>();
      temp.put("count", entry.getValue().size());
      temp.put("avgRunTime", avgForAction);
      actionToStatistic.put(entry.getKey(), temp);
    }

    for (Map.Entry<String, List<Long>> entry : actionToRunRecord.entrySet()) {
      List<Long> runList = entry.getValue();
      Collections.sort(runList);
      for (double percentile : percentiles) {
        long percentileValue = runList.get((int) ((percentile * runList.size()) / 100));
        actionToStatistic.get(entry.getKey()).put(Double.toString(percentile), (double) percentileValue);
      }
    }

    BasicStatistics basicStatistics = new BasicStatistics(startTime, endTime, count, avgRunTime, percentileToTime,
                                                          percentileToRunids, actionToStatistic);

    return basicStatistics;
  }

  private Pair<Map<String, Long>, Map<String, List<String>>> getPercentiles(List<WorkflowRunRecord> workflowRunRecords,
                                                                            List<Double> percentiles) {
    int count = workflowRunRecords.size();
    Map<String, Long> percentileToTime = new HashMap<>();
    Map<String, List<String>> percentileToRunids = new HashMap<>();
    for (double i : percentiles) {
      List<String> percentileRun = new ArrayList();
      int percentileStart = (int) ((i * count) / 100);
      for (int j = percentileStart; j < count; j++) {
        percentileRun.add(workflowRunRecords.get(j).getWorkflowRunId());
      }
      percentileToRunids.put(Double.toString(i), percentileRun);
      percentileToTime.put(Double.toString(i), workflowRunRecords.get(percentileStart).getTimeTaken());
    }
    return new Pair<>(percentileToTime, percentileToRunids);
  }

  private List<WorkflowDataset.WorkflowRunRecord> sort(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Collections.sort(workflowRunRecords, new Comparator<WorkflowRunRecord>() {
      @Override
      public int compare(WorkflowDataset.WorkflowRunRecord o1, WorkflowDataset.WorkflowRunRecord o2) {
        return Longs.compare(o1.getTimeTaken(), o2.getTimeTaken());
      }
    });
    return workflowRunRecords;
  }

  private Map<String, List<Long>> getActionRuns(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Map<String, List<Long>> actionToRunRecord = new HashMap<>();
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      for (WorkflowDataset.ActionRuns runs : workflowRunRecord.getActionRuns()) {
        List<Long> runList = actionToRunRecord.get(runs.getName());
        if (runList == null) {
          runList = new ArrayList<>();
          actionToRunRecord.put(runs.getName(), runList);
        } else {
          runList = actionToRunRecord.get(runs.getName());
        }
        runList.add(runs.getTimeTaken());
      }
    }
    return actionToRunRecord;
  }

  /**
   * Internal class to keep track of Workflow Run Records
   */
  static class WorkflowRunRecord {
    private final String workflowRunId;
    private final long timeTaken;
    private final List<ActionRuns> actionRuns;

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
  static class ActionRuns {
    private final String runId;
    private final long timeTaken;
    private final ProgramType programType;
    private final String name;

    public ActionRuns(String name, String runId, ProgramType programType, long timeTaken) {
      this.name = name;
      this.runId = runId;
      this.programType = programType;
      this.timeTaken = timeTaken;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public long getTimeTaken() {
      return timeTaken;
    }

    public String getName() {
      return name;
    }

    public String getRunId() {
      return runId;
    }
  }

  /**
   * Internal class to create Statistics
   */
  @VisibleForTesting
  public static class BasicStatistics {
    private final long startTime;
    private final long endTime;
    private final int runs;
    private final double avgRunTime;
    private final Map<String, Long> percentileToTime;
    private final Map<String, List<String>> percentileToRunids;
    private final Map<String, Map<String, Double>> actionToStatistic;

    public BasicStatistics(long startTime, long endTime, int runs, double avgRunTime,
                           Map<String, Long> percentileToTime,
                           Map<String, List<String>> percentileToRunids,
                           Map<String, Map<String, Double>> actionToStatistic) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.runs = runs;
      this.avgRunTime = avgRunTime;
      this.percentileToTime = percentileToTime;
      this.percentileToRunids = percentileToRunids;
      this.actionToStatistic = actionToStatistic;
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

    public Map<String, Map<String, Double>> getActionToStatistic() {
      return actionToStatistic;
    }
  }
}
