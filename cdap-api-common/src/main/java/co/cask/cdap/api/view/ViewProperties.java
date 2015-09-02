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
package co.cask.cdap.api.view;

import java.util.Objects;

/**
 * Represents the properties of a view.
 */
public class ViewProperties {

  /**
   * The config used to create the view.
   */
  private final ViewConfig config;
  /**
   * The select statement associated with the view.
   */
  private final String selectStatement;

  public ViewProperties(ViewConfig config, String selectStatement) {
    this.config = config;
    this.selectStatement = selectStatement;
  }

  public ViewConfig getConfig() {
    return config;
  }

  public String getSelectStatement() {
    return selectStatement;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ViewProperties that = (ViewProperties) o;
    return Objects.equals(config, that.config) &&
      Objects.equals(selectStatement, that.selectStatement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, selectStatement);
  }
}
