/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.client;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.explore.service.UnexpectedQueryStatusException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamViewProperties;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Explore client facade to be used by streams and datasets.
 */
public class ExploreFacade {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreFacade.class);

  private final ExploreClient exploreClient;
  private final boolean exploreEnabled;

  @Inject
  public ExploreFacade(ExploreClient exploreClient, CConfiguration cConf) {
    this.exploreClient = exploreClient;
    this.exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    if (!exploreEnabled) {
      LOG.warn("Explore functionality for datasets is disabled. All calls to enable explore will be no-ops");
    }
  }

  /**
   * Creates or updates a table for a stream view.
   *
   * @param view the stream view
   * @param properties the stream view properties
   */
  public void createOrUpdateStreamViewTable(Id.Stream.View view, StreamViewProperties properties)
    throws ExploreException, SQLException {

    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.createOrUpdateStreamViewTable(view, properties);
    handleExploreFuture(futureSuccess, "createOrUpdate", "stream-view", view);
  }

  /**
   * Deletes the table associated with a stream view.
   *
   * @param view the stream view
   */
  public void deleteStreamViewTable(Id.Stream.View view) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.deleteStreamViewTable(view);
    handleExploreFuture(futureSuccess, "delete", "stream-view", view);
  }

  public boolean streamViewTableExists(Id.Stream.View viewId) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return false;
    }

    ListenableFuture<Boolean> futureSuccess = exploreClient.streamViewTableExists(viewId);
    return handleExploreFuture(futureSuccess, "exists", "stream-view", viewId);
  }

  /**
   * Enables ad-hoc exploration of the given stream.
   *
   * @param stream id of the stream.
   *
   * @deprecated As of 3.2.0, multiple tables can be associated with a stream via stream views.
   */
  @Deprecated
  public void enableExploreStream(Id.Stream stream) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.enableExploreStream(stream);
    handleExploreFuture(futureSuccess, "enable", "stream", stream);
  }

  /**
   * Disables ad-hoc exploration of the given stream.
   *
   * @param stream id of the stream.
   *
   * @deprecated As of 3.2.0, multiple tables can be associated with a stream via stream views.
   */
  @Deprecated
  public void disableExploreStream(Id.Stream stream) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.disableExploreStream(stream);
    handleExploreFuture(futureSuccess, "disable", "stream", stream);
  }

  /**
   * Enables ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance id.
   */
  public void enableExploreDataset(Id.DatasetInstance datasetInstance) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.enableExploreDataset(datasetInstance);
    handleExploreFuture(futureSuccess, "enable", "dataset", datasetInstance);
  }

  /**
   * Disable ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance id.
   */
  public void disableExploreDataset(Id.DatasetInstance datasetInstance) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.disableExploreDataset(datasetInstance);
    handleExploreFuture(futureSuccess, "disable", "dataset", datasetInstance);
  }

  public void addPartition(Id.DatasetInstance datasetInstance,
                           PartitionKey key, String location) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.addPartition(datasetInstance, key, location);
    handleExploreFuture(futureSuccess, "add", "partition", datasetInstance);
  }

  public void dropPartition(Id.DatasetInstance datasetInstance,
                            PartitionKey key) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.dropPartition(datasetInstance, key);
    handleExploreFuture(futureSuccess, "drop", "partition", datasetInstance);
  }

  public void createNamespace(Id.Namespace namespace) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<ExploreExecutionResult> futureSuccess = exploreClient.addNamespace(namespace);
    handleExploreFuture(futureSuccess, "add", "namespace", namespace);
  }

  public void removeNamespace(Id.Namespace namespace) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<ExploreExecutionResult> futureSuccess = exploreClient.removeNamespace(namespace);
    handleExploreFuture(futureSuccess, "remove", "namespace", namespace);
  }

  private boolean isDatasetExplorable(Id.DatasetInstance datasetInstance) {
    return !Id.Namespace.SYSTEM.equals(datasetInstance.getNamespace());
  }

  // wait for the enable/disable operation to finish and log and throw exceptions as appropriate if there was an error.
  private <T> T handleExploreFuture(ListenableFuture<T> future, String operation, String type, Id objectId)
    throws ExploreException, SQLException {
    try {
      return future.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    } catch (ExecutionException e) {
      Throwable t = Throwables.getRootCause(e);
      if (t instanceof ExploreException) {
        LOG.error("{} explore did not finish successfully for {}: {}.",
                  operation, type, objectId.toString());
        throw (ExploreException) t;
      } else if (t instanceof SQLException) {
        throw (SQLException) t;
      } else if (t instanceof HandleNotFoundException) {
        // Cannot happen unless explore server restarted, or someone calls close in between.
        LOG.error("Error running {} explore", operation, e);
        throw Throwables.propagate(e);
      } else if (t instanceof UnexpectedQueryStatusException) {
        UnexpectedQueryStatusException sE = (UnexpectedQueryStatusException) t;
        LOG.error("{} explore operation ended in an unexpected state - {}", operation, sE.getStatus().name(), e);
        throw Throwables.propagate(e);
      } else {
        throw Throwables.propagate(t);
      }
    } catch (TimeoutException e) {
      LOG.error("Error running {} explore - operation timed out", operation, e);
      throw Throwables.propagate(e);
    }
  }
}
