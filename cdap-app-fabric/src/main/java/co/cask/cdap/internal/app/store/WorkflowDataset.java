package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class WorkflowDataset extends AbstractDataset {

  private static final Gson GSON = new Gson();
  private static final String RUNID = "runId";
  private static final String TIME_TAKEN = "timeTaken";
  private static final String NODES = "nodes";
  private static final String DELIMITER = ":";
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDataset.class);

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

    String value = GSON.toJson(actionRunsList, new TypeToken<List<ActionRuns>>() { }.getType());

    table.put(rowKey, Bytes.toBytes(RUNID), Bytes.toBytes(runRecordMeta.getPid()));
    table.put(rowKey, Bytes.toBytes(TIME_TAKEN), Bytes.toBytes(timeTaken));
    table.put(rowKey, Bytes.toBytes(NODES), Bytes.toBytes(value));
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
      String workflowRunId = Bytes.toString(columns.get(Bytes.toBytes(RUNID)));
      long timeTaken = Bytes.toLong(columns.get(Bytes.toBytes(TIME_TAKEN)));

      List<ActionRuns> actionRunsList = GSON.fromJson(Bytes.toString(columns.get(Bytes.toBytes(NODES))),
                                                      new TypeToken<List<ActionRuns>>() { }.getType());
      WorkflowRunRecord workflowRunRecord = new WorkflowRunRecord(workflowRunId, timeTaken, actionRunsList);
      workflowRunRecordList.add(workflowRunRecord);
    }
    return workflowRunRecordList;
  }

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
