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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.Id;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Handles dataset instance management calls.
 */
public interface DatasetInstanceService {

  /**
   * Lists all dataset instances in a namespace.
   *
   * @param namespace the namespace to list datasets for
   * @return the dataset instances in the provided namespace
   * @throws NotFoundException if the namespace was not found
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists.
   */
  Collection<DatasetSpecification> list(Id.Namespace namespace) throws Exception;

  /**
   * Gets a dataset instance.
   *
   * @param instance instance to get
   * @param owners the {@link Id}s that will be using the dataset instance
   * @return the dataset instance's {@link DatasetMeta}
   * @throws NotFoundException if either the namespace or dataset instance is not found,
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists.
   */
  DatasetMeta get(Id.DatasetInstance instance, @Nullable Iterable<? extends Id> owners) throws Exception;

  /**
   * Gets a dataset instance.
   *
   * @param instance instance to get
   * @return the dataset instance's {@link DatasetMeta}
   * @throws NotFoundException if either the namespace or dataset instance is not found,
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists.
   */
  DatasetMeta get(Id.DatasetInstance instance) throws Exception;

  /**
   * Creates a dataset instance.
   *
   * @param namespace the namespace to create the dataset instance in
   * @param name the name of the new dataset instance
   * @param props the properties for the new dataset instance
   * @throws NamespaceNotFoundException if the specified namespace was not found
   * @throws DatasetAlreadyExistsException if a dataset with the same name already exists
   * @throws DatasetTypeNotFoundException if the dataset type was not found
   * @throws Exception if something went wrong
   */
  void create(Id.Namespace namespace, String name, DatasetInstanceConfiguration props) throws Exception;

  /**
   * Updates an existing Dataset specification properties.
   * {@link DatasetInstanceConfiguration} is constructed based on request and the Dataset instance is updated.
   *
   * @param instance the dataset instance
   * @param properties the dataset properties to be used
   * @throws NamespaceNotFoundException if the specified namespace was not found
   * @throws DatasetNotFoundException if the dataset was not found
   * @throws DatasetTypeNotFoundException if the type of the existing dataset was not found
   */
  void update(Id.DatasetInstance instance, Map<String, String> properties) throws Exception;

  /**
   * Drops the specified dataset instance.
   *
   * @param instance the {@link Id.DatasetInstance} to drop
   * @throws NamespaceNotFoundException if the namespace was not found
   * @throws DatasetNotFoundException if the dataset instance was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   */
  void delete(Id.DatasetInstance instance) throws Exception;

  /**
   * Executes an admin operation on a dataset instance.
   *
   * @param instance the instance to execute the admin operation on
   * @param method the type of admin operation to execute
   * @return the {@link DatasetAdminOpResponse} from the HTTP handler
   * @throws NamespaceNotFoundException if the requested namespace was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   */
  DatasetAdminOpResponse executeAdmin(Id.DatasetInstance instance, String method) throws Exception;
}
