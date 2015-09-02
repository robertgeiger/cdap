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
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link ExploreViewStore}.
 */
public final class InMemoryExploreViewStore implements ExploreViewStore {

  private final Map<Id.View, ViewProperties> views;
  private final Multimap<Id.Namespace, Id.View> viewsByNamespace;

  public InMemoryExploreViewStore() {
    views = Maps.newHashMap();
    viewsByNamespace = HashMultimap.create();
  }

  @Override
  public boolean createOrUpdate(Id.View viewId, ViewProperties properties) {
    boolean created = !views.containsKey(viewId);
    views.put(viewId, properties);
    viewsByNamespace.put(viewId.getNamespace(), viewId);
    return created;
  }

  @Override
  public boolean exists(Id.View viewId) {
    return views.containsKey(viewId);
  }

  @Override
  public void delete(Id.View viewId) throws NotFoundException {
    if (!views.containsKey(viewId)) {
      throw new NotFoundException(viewId);
    }

    views.remove(viewId);
    viewsByNamespace.remove(viewId.getNamespace(), viewId);
  }

  @Override
  public List<String> list(Id.Namespace namespaceId) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    builder.addAll(Collections2.transform(viewsByNamespace.get(namespaceId), new Function<Id.View, String>() {
      @Nullable
      @Override
      public String apply(Id.View input) {
        return input.getId();
      }
    }));
    return builder.build();
  }

  @Override
  public ViewDetail get(Id.View viewId) throws NotFoundException {
    if (!views.containsKey(viewId)) {
      throw new NotFoundException(viewId);
    }

    ViewProperties properties = views.get(viewId);
    return new ViewDetail(viewId.getId(), properties.getSelectStatement());
  }
}
