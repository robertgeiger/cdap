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

package co.cask.cdap.data2.datafabric.dataset.type;

import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.util.concurrent.Service;
import org.apache.twill.filesystem.Location;

import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Manages dataset types and modules metadata
 */
public interface DatasetTypeService extends Service {

  /**
   * Add datasets module in a namespace
   *
   * @param datasetModuleId the {@link Id.DatasetModule} to add
   * @param className module class
   * @param jarLocation location of the module jar
   */
  void addModule(final Id.DatasetModule datasetModuleId, final String className,
                 final Location jarLocation) throws DatasetModuleConflictException;

  /**
   *
   * @param namespaceId the {@link Id.Namespace} to retrieve types from
   * @return collection of types available in the specified namespace
   */
  Collection<DatasetTypeMeta> getTypes(final Id.Namespace namespaceId);

  /**
   * Get dataset type information
   * @param datasetTypeId name of the type to get info for
   * @return instance of {@link DatasetTypeMeta} or {@code null} if type
   *         does NOT exist
   */
  @Nullable
  DatasetTypeMeta getTypeInfo(final Id.DatasetType datasetTypeId);

  /**
   * @param namespaceId {@link Id.Namespace} to retrieve the module list from
   * @return list of dataset modules information from the specified namespace
   */
  Collection<DatasetModuleMeta> getModules(final Id.Namespace namespaceId);

  /**
   * @param datasetModuleId {@link Id.DatasetModule} of the module to return info for
   * @return dataset module info or {@code null} if module with given name does NOT exist
   */
  @Nullable
  DatasetModuleMeta getModule(final Id.DatasetModule datasetModuleId);

  /**
   * Deletes specified dataset module
   * @param datasetModuleId {@link Id.DatasetModule} of the dataset module to delete
   * @return true if deleted successfully, false if module didn't exist: nothing to delete
   * @throws DatasetModuleConflictException when there are other modules depend on the specified one, in which case
   *         deletion does NOT happen
   */
  boolean deleteModule(final Id.DatasetModule datasetModuleId) throws DatasetModuleConflictException;

  /**
   * Deletes all modules in a namespace, other than system.
   * Presumes that the namespace has already been checked to be non-system.
   *
   * @param namespaceId the {@link Id.Namespace} to delete modules from.
   */
  void deleteModules(final Id.Namespace namespaceId) throws DatasetModuleConflictException;
}
