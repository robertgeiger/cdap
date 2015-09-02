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

package co.cask.cdap.proto;

import co.cask.cdap.api.view.ViewConfig;

/**
 * Represents a view in an HTTP response.
 */
public class ViewDetail {

  private final String id;
  private final ViewConfig config;
  private final String selectStatement;

  public ViewDetail(String id, ViewConfig config, String selectStatement) {
    this.id = id;
    this.config = config;
    this.selectStatement = selectStatement;
  }

  public String getId() {
    return id;
  }

  public ViewConfig getConfig() {
    return config;
  }

  public String getSelectStatement() {
    return selectStatement;
  }
}
