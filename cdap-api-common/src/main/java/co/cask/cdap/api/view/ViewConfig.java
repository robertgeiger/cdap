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

import java.util.Map;

/**
 * Represents the configuration of a view, used when creating or updating a view.
 */
public class ViewConfig {

  private final String format;
  private final Map<String, String> config;

  public ViewConfig(String format, Map<String, String> config) {
    this.format = format;
    this.config = config;
  }

  public String getFormat() {
    return format;
  }

  public Map<String, String> getConfig() {
    return config;
  }
}
