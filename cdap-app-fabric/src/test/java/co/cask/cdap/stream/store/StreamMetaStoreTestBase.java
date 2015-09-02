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

package co.cask.cdap.stream.store;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamViewProperties;
import co.cask.cdap.proto.StreamViewSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * Test the {@link StreamMetaStore} implementations.
 */
public abstract class StreamMetaStoreTestBase {

  protected abstract StreamMetaStore getStreamMetaStore();

  protected abstract void createNamespace(String namespaceId) throws AlreadyExistsException;

  protected abstract void deleteNamespace(String namespaceId) throws NotFoundException;

  @Before
  public void beforeTests() throws Exception {
    createNamespace("foo");
    createNamespace("foo1");
    createNamespace("foo2");
  }

  @After
  public void afterTests() throws Exception {
    deleteNamespace("foo");
    deleteNamespace("foo1");
    deleteNamespace("foo2");
  }

  @Test
  public void testStreamMetastore() throws Exception {
    StreamMetaStore streamMetaStore = getStreamMetaStore();

    streamMetaStore.addStream(Id.Stream.from("foo", "bar"));
    Assert.assertTrue(streamMetaStore.streamExists(Id.Stream.from("foo", "bar")));
    Assert.assertFalse(streamMetaStore.streamExists(Id.Stream.from("foofoo", "bar")));

    streamMetaStore.removeStream(Id.Stream.from("foo", "bar"));
    Assert.assertFalse(streamMetaStore.streamExists(Id.Stream.from("foo", "bar")));

    streamMetaStore.addStream(Id.Stream.from("foo1", "bar"));
    streamMetaStore.addStream(Id.Stream.from("foo2", "bar"));
    Assert.assertEquals(ImmutableList.of(
                          new StreamSpecification.Builder().setName("bar").create()),
                        streamMetaStore.listStreams(Id.Namespace.from("foo1")));
    Assert.assertEquals(
      ImmutableMultimap.builder()
        .put(Id.Namespace.from("foo1"), new StreamSpecification.Builder().setName("bar").create())
        .put(Id.Namespace.from("foo2"), new StreamSpecification.Builder().setName("bar").create())
        .build(),
      streamMetaStore.listStreams());

    streamMetaStore.removeStream(Id.Stream.from("foo2", "bar"));
    Assert.assertFalse(streamMetaStore.streamExists(Id.Stream.from("foo2", "bar")));
    Assert.assertEquals(
      ImmutableMultimap.builder()
        .put(Id.Namespace.from("foo1"), new StreamSpecification.Builder().setName("bar").create())
        .build(),
      streamMetaStore.listStreams());

    streamMetaStore.removeStream(Id.Stream.from("foo1", "bar"));

    // test stream view
    FormatSpecification formatSpec = new FormatSpecification(
      Formats.AVRO, null, Collections.<String, String>emptyMap());
    streamMetaStore.addStreamView(Id.Stream.View.from("foo", "view1"),
                                  new StreamViewProperties(Id.Stream.from("foo", "stream1"), formatSpec));

    Assert.assertEquals(
      Lists.newArrayList(new StreamViewSpecification("view1", formatSpec)),
      streamMetaStore.listStreamViews(Id.Namespace.from("foo")));
    streamMetaStore.removeStreamView(Id.Stream.View.from("foo", "view1"));
  }
}
