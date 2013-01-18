package com.continuuity.data.dataset;

import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.WriteOperation;

import java.util.LinkedList;
import java.util.List;

public class SimpleBatchCollector implements BatchCollector {

  List<WriteOperation> ops = new LinkedList<WriteOperation>();

  @Override
  public void add(WriteOperation write) {
    this.ops.add(write);
  }

  public List<WriteOperation> getWrites() {
    return this.ops;
  }
}
