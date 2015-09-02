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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test the {@link ExploreViewStore} implementations.
 */
public abstract class ExploreViewStoreTestBase {

  protected abstract ExploreViewStore getExploreViewStore();

  @Test
  public void testExploreViewStore() throws Exception {
    ExploreViewStore store = getExploreViewStore();

    Id.Namespace namespace = Id.Namespace.from("foo");
    Id.View view1 = Id.View.from(namespace, "bar1");
    Id.View view2 = Id.View.from(namespace, "bar2");
    Id.View view3 = Id.View.from(namespace, "bar3");

    Assert.assertFalse(store.exists(view1));

    ViewProperties properties = new ViewProperties(null, "select * from stream_baz");
    Assert.assertTrue("view1 should be created", store.createOrUpdate(view1, properties));
    Assert.assertTrue("view1 should exist", store.exists(view1));
    Assert.assertEquals("view1 should have the initial properties",
                        new ViewDetail(view1.getId(), properties.getSelectStatement()), store.get(view1));

    ViewProperties properties2 = new ViewProperties(null, "select * from stream_foo");
    Assert.assertFalse("view1 should be updated", store.createOrUpdate(view1, properties2));
    Assert.assertTrue("view1 should exist", store.exists(view1));
    Assert.assertEquals("view1 should have the updated properties",
                        new ViewDetail(view1.getId(), properties2.getSelectStatement()), store.get(view1));

    Assert.assertTrue("view2 should be created", store.createOrUpdate(view2, properties));
    Assert.assertTrue("view3 should be created", store.createOrUpdate(view3, properties));
    Assert.assertEquals("view1, view2, and view3 should be in the namespace",
                        ImmutableList.of(view1.getId(), view2.getId(), view3.getId()),
                        Ordering.natural().immutableSortedCopy(store.list(namespace)));

    store.delete(view1);
    Assert.assertFalse(store.exists(view1));

    store.delete(view2);
    Assert.assertFalse(store.exists(view2));

    store.delete(view3);
    Assert.assertFalse(store.exists(view3));

    Assert.assertEquals(ImmutableList.of(), store.list(namespace));
  }
}
