/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction;

import co.cask.tephra.AbstractTransactionExecutor;
import co.cask.tephra.RetryOnConflictStrategy;
import co.cask.tephra.RetryStrategies;
import co.cask.tephra.RetryStrategy;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Utility class that encapsulates the transaction life cycle over a given set of
 * transaction-aware datasets. The executor can be reused across multiple invocations
 * of the execute() method. However, it is not thread-safe for concurrent execution.
 * <p>
 *   Transaction execution will be retries according to specified in constructor {@link RetryStrategy}.
 *   By default {@link RetryOnConflictStrategy} is used with max 20 retries and 100 ms between retries.
 * </p>
 */
public class DynamicTransactionExecutor extends AbstractTransactionExecutor {

  private final Supplier<TransactionContext> txContextSupplier;
  private final RetryStrategy retryStrategy;

  public DynamicTransactionExecutor(Supplier<TransactionContext> txContextSupplier, RetryStrategy retryStrategy) {
    super(MoreExecutors.sameThreadExecutor());
    this.txContextSupplier = txContextSupplier;
    this.retryStrategy = retryStrategy;
  }

  public DynamicTransactionExecutor(Supplier<TransactionContext> txContextSupplier) {
    this(txContextSupplier, RetryStrategies.retryOnConflict(20, 100));
  }

  public DynamicTransactionExecutor(final TransactionSystemClient txClient,
                                    final Iterable<TransactionAware> txAwares,
                                    RetryStrategy retryStrategy) {
    this(Transactions.constantContextSupplier(txClient, txAwares), retryStrategy);
  }

  public DynamicTransactionExecutor(TransactionSystemClient txClient, TransactionAware... txAwares) {
    this(txClient, Arrays.asList(txAwares));
  }

  @Inject
  public DynamicTransactionExecutor(TransactionSystemClient txClient, @Assisted Iterable<TransactionAware> txAwares) {
    this(Transactions.constantContextSupplier(txClient, txAwares));
  }

  @Override
  public <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException, InterruptedException {
    return executeWithRetry(function, input);
  }

  @Override
  public <I> void execute(final Procedure<I> procedure, I input)
    throws TransactionFailureException, InterruptedException {

    execute(new Function<I, Void>() {
      @Override
      public Void apply(I input) throws Exception {
        procedure.apply(input);
        return null;
      }
    }, input);
  }

  @Override
  public <O> O execute(final Callable<O> callable) throws TransactionFailureException, InterruptedException {
    return execute(new Function<Void, O>() {
      @Override
      public O apply(Void input) throws Exception {
        return callable.call();
      }
    }, null);
  }

  @Override
  public void execute(final Subroutine subroutine) throws TransactionFailureException, InterruptedException {
    execute(new Function<Void, Void>() {
      @Override
      public Void apply(Void input) throws Exception {
        subroutine.apply();
        return null;
      }
    }, null);
  }

  private <I, O> O executeWithRetry(Function<I, O> function, I input)
    throws TransactionFailureException, InterruptedException {

    int retries = 0;
    while (true) {
      try {
        return executeOnce(function, input);
      } catch (TransactionFailureException e) {
        long delay = retryStrategy.nextRetry(e, ++retries);

        if (delay < 0) {
          throw e;
        }

        if (delay > 0) {
          TimeUnit.MILLISECONDS.sleep(delay);
        }
      }
    }

  }

  private <I, O> O executeOnce(Function<I, O> function, I input) throws TransactionFailureException {
    TransactionContext txContext = txContextSupplier.get();
    txContext.start();
    O o = null;
    try {
      o = function.apply(input);
    } catch (Throwable e) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", e));
      // abort will throw
    }
    // will throw if smth goes wrong
    txContext.finish();
    return o;
  }
}
