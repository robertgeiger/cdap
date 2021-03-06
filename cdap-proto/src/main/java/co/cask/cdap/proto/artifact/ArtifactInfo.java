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

package co.cask.cdap.proto.artifact;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.artifact.ArtifactClasses;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;

import java.util.Objects;

/**
 * Represents an artifact returned by /artifacts/{artifact-name}/versions/{artifact-version}.
 */
@Beta
public class ArtifactInfo extends ArtifactSummary {
  private final ArtifactClasses classes;

  public ArtifactInfo(ArtifactId id, ArtifactClasses classes) {
    this(id.getName(), id.getVersion().getVersion(), id.getScope(), classes);
  }

  public ArtifactInfo(String name, String version, ArtifactScope scope, ArtifactClasses classes) {
    super(name, version, scope);
    this.classes = classes;
  }

  public ArtifactClasses getClasses() {
    return classes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactInfo that = (ArtifactInfo) o;

    return super.equals(that) &&
      Objects.equals(classes, that.classes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), classes);
  }

  @Override
  public String toString() {
    return "ArtifactInfo{" +
      "name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", scope=" + scope +
      ", classes=" + classes +
      '}';
  }
}
