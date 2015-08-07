package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.Id;

/**
 *
 */
public class WorkflowDataset extends AbstractDataset {

  private final Table table;

  public WorkflowDataset(Table table) {
    super("ignored", table);
    this.table = table;
  }

  public void write(RunRecordMeta runRecordMeta) {
    String pid = runRecordMeta.getPid();
    long start = runRecordMeta.getStartTs();
    long stop = runRecordMeta.getStopTs();
    runRecordMeta.getProperties();
  }

}
