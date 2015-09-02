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

import java.util.Objects;

/**
 * Represents a view in an HTTP response.
 */
public class ViewDetail {

  private final String id;
  private final String selectStatement;

  public ViewDetail(String id, String selectStatement) {
    this.id = id;
    this.selectStatement = selectStatement;
  }

  public String getId() {
    return id;
  }

  public String getSelectStatement() {
    return selectStatement;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ViewDetail that = (ViewDetail) o;
    return Objects.equals(id, that.id) &&
      Objects.equals(selectStatement, that.selectStatement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, selectStatement);
  }
}
