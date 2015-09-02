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

package co.cask.cdap.explore.store;

import co.cask.cdap.api.view.ViewProperties;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;

import java.util.List;

/**
 * Interface for storing Explore views.
 */
public interface ExploreViewStore {

  /**
   * Creates a view. Updates the view if it already exists.
   * @param viewId the view
   * @param properties the view properties
   * @return true if a new view was created
   */
  boolean createOrUpdate(Id.View viewId, ViewProperties properties);

  /**
   * @param viewId the view
   * @return true if the view exists
   */
  boolean exists(Id.View viewId);

  /**
   * Deletes a view.
   *
   * @param viewId the view
   */
  void delete(Id.View viewId) throws NotFoundException;

  /**
   * @param namespaceId the namespace
   * @return list of view IDs in a namespace
   */
  List<String> list(Id.Namespace namespaceId);

  /**
   * @param viewId the view
   * @return the details of a view
   */
  ViewDetail get(Id.View viewId) throws NotFoundException;
}
