/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.api.view.ViewConfig;
import co.cask.cdap.api.view.ViewProperties;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreTableManager;
import co.cask.cdap.explore.store.ExploreViewStore;
import co.cask.cdap.explore.view.RawViewConfigFormat;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.spi.view.ViewConfigFormat;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements namespaced explore metadata APIs.
 *
 * TODO: Decide how to handle views that are missing corresponding Hive views.
 * Currently we assume a Hive view exists for each view in the {@link ExploreViewStore}.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace}/data/explore")
public class ExploreViewHttpHandler extends AbstractExploreMetadataHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreViewHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final ExploreTableManager tableManager;
  private final ExploreViewStore store;
  private final Map<String, ViewConfigFormat> formats = ImmutableMap.<String, ViewConfigFormat>builder()
    .put("raw", new RawViewConfigFormat())
    .build();

  @Inject
  public ExploreViewHttpHandler(ExploreTableManager tableManager, ExploreViewStore store) {
    this.tableManager = tableManager;
    this.store = store;
  }

  @PUT
  @Path("/views/{view}")
  public void createOrUpdate(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace") String namespace,
                             @PathParam("view") String view) throws Exception {

    Id.View viewId = Id.View.from(namespace, view);
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()))) {
      ViewConfig config = GSON.fromJson(reader, ViewConfig.class);
      if (!formats.containsKey(config.getFormat())) {
        throw new BadRequestException("Invalid format: " + config.getFormat());
      }

      ViewConfigFormat format = formats.get(config.getFormat());
      ViewProperties properties = format.transform(config);
      tableManager.createOrReplaceView(viewId, properties);
      boolean created = store.createOrUpdate(viewId, properties);
      if (created) {
        responder.sendStatus(HttpResponseStatus.CREATED);
      } else {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (IOException e) {
      LOG.warn("Error closing InputStreamReader", e);
    }
  }

  @DELETE
  @Path("/views/{view}")
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace") String namespace,
                     @PathParam("view") String view) throws Exception {

    Id.View viewId = Id.View.from(namespace, view);
    if (!store.exists(viewId)) {
      throw new NotFoundException(viewId);
    }

    tableManager.delete(viewId);
    store.delete(viewId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/views")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace") String namespace) {

    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    List<String> list = store.list(namespaceId);
    responder.sendJson(HttpResponseStatus.OK, list);
  }

  @GET
  @Path("/views/{view}")
  public void get(HttpRequest request, HttpResponder responder,
                  @PathParam("namespace") String namespace,
                  @PathParam("view") String view) throws NotFoundException {

    Id.View viewId = Id.View.from(namespace, view);
    ViewDetail detail = store.get(viewId);
    responder.sendJson(HttpResponseStatus.OK, detail);
  }
}
