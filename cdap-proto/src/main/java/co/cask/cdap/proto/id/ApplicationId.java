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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.ElementType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies an application.
 */
public class ApplicationId extends ElementId implements NamespacedId, ParentedId<NamespaceId> {
  private final String namespace;
  private final String application;

  public ApplicationId(String namespace, String application) {
    super(ElementType.APPLICATION);
    this.namespace = namespace;
    this.application = application;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  public ProgramId program(ProgramType type, String program) {
    return new ProgramId(namespace, application, type, program);
  }

  @Override
  public Id toId() {
    return Id.Application.from(namespace, application);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ApplicationId that = (ApplicationId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(application, that.application);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, application);
  }

  @SuppressWarnings("unused")
  public static ApplicationId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ApplicationId(safeNext(iterator, "namespace"), safeNextAndEnd(iterator, "application"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, application);
  }

  public static ApplicationId fromString(String string) {
    return ElementId.fromString(string, ApplicationId.class);
  }
}
