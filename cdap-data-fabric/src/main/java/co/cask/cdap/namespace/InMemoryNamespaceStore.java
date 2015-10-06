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

package co.cask.cdap.namespace;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * An in-memory implementation of {@link NamespaceStore} to be used in tests. This is used in tests where
 * AppFabricServer (which runs the namespaces REST APIs) is not started, and an implementation of
 * {@link co.cask.cdap.data2.dataset2.DatasetFramework} is not available at creation.
 */
@Singleton
public class InMemoryNamespaceStore implements NamespaceStore {

  private final List<NamespaceMeta> namespaces = new ArrayList<>();

  @Nullable
  @Override
  public NamespaceMeta create(NamespaceMeta metadata) {
    namespaces.add(metadata);
    return metadata;
  }

  @Override
  public void update(NamespaceMeta metadata) {
    delete(Id.Namespace.from(metadata.getName()));
    create(metadata);
  }

  @Nullable
  @Override
  public NamespaceMeta get(final Id.Namespace id) {
    Iterable<NamespaceMeta> filtered = Iterables.filter(namespaces, new Predicate<NamespaceMeta>() {
      @Override
      public boolean apply(NamespaceMeta input) {
        return input.getName().equals(id.getId());
      }
    });
    return filtered.iterator().next();
  }

  @Nullable
  @Override
  public NamespaceMeta delete(Id.Namespace id) {
    Iterator<NamespaceMeta> it = namespaces.iterator();
    while (it.hasNext()) {
      NamespaceMeta namespace = it.next();
      if (namespace.getName().equals(id.getId())) {
        it.remove();
        return namespace;
      }
    }
    return null;
  }

  @Override
  public List<NamespaceMeta> list() {
    return namespaces;
  }

  @Override
  public boolean exists(Id.Namespace id) {
    return get(id) != null;
  }
}
