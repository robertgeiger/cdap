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
import co.cask.cdap.proto.element.ElementType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * TODO: toString() must be namespace.category.feed for backwards compatibility with Id.NotificationFeed
 */
public class NotificationFeedId extends ElementId implements NamespacedId, ParentedId<NamespaceId> {
  private final String namespace;
  private final String category;
  private final String feed;

  public NotificationFeedId(String namespace, String category, String feed) {
    super(ElementType.NOTIFICATION_FEED);
    this.namespace = namespace;
    this.category = category;
    this.feed = feed;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getCategory() {
    return category;
  }

  public String getFeed() {
    return feed;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NotificationFeedId that = (NotificationFeedId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(category, that.category) &&
      Objects.equals(feed, that.feed);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, category, feed);
  }

  @Override
  public Id toId() {
    return Id.NotificationFeed.from(namespace, category, feed);
  }

  @SuppressWarnings("unused")
  public static NotificationFeedId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new NotificationFeedId(
      safeNext(iterator, "namespace"), safeNext(iterator, "category"),
      safeNextAndEnd(iterator, "feed"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, category, feed);
  }
}
