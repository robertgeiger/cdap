package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data2.OperationException;
import com.google.common.collect.Iterables;

import java.util.Iterator;

/**
 * A {@link QueueReader} that reads from a list of {@link QueueReader}
 * in Round-Robin fashion. It will try skipping empty inputs when dequeueing
 * until a non-empty one is found or has exhausted the list of underlying
 * {@link QueueReader}, which will return an empty input.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public final class RoundRobinQueueReader<T> implements QueueReader<T> {

  private final InputDatum<T> nullInput = new NullInputDatum<T>();
  private final Iterator<QueueReader<T>> readers;

  public RoundRobinQueueReader(Iterable<QueueReader<T>> readers) {
    this.readers = Iterables.cycle(readers).iterator();
  }

  @Override
  public InputDatum<T> dequeue() throws OperationException {
    if (!readers.hasNext()) {
      return nullInput;
    }

    // Read an input from the underlying QueueReader
    QueueReader<T> begin = readers.next();
    InputDatum<T> input = begin.dequeue();

    // While the input is empty, keep trying to read from subsequent readers,
    // until a non-empty input is read or it loop back to the beginning reader.
    while (!input.needProcess()) {
      QueueReader<T> reader = readers.next();
      if (reader == begin) {
        return input;
      }
      input = reader.dequeue();
    }
    return input;
  }
}
