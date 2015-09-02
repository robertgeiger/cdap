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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.validator.Validator;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamViewProperties;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST call to V3 stream view APIs.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public final class StreamViewHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamViewHandler.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final StreamAdmin streamAdmin;

  @Inject
  public StreamViewHandler(StreamAdmin streamAdmin) {
    this.streamAdmin = streamAdmin;
  }

  @PUT
  @Path("/views/stream/{view}")
  public void createOrUpdateView(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("view") String view) throws Exception {

    Id.Stream.View viewId = Id.Stream.View.from(namespaceId, view);

    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    StreamViewProperties properties = getAndValidateViewConfig(reader);
    checkStreamExists(properties.getStream());

    boolean created = streamAdmin.createOrUpdateView(viewId, properties);
    if (created) {
      responder.sendStatus(HttpResponseStatus.CREATED);
    } else {
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @DELETE
  @Path("/views/stream/{view}")
  public void deleteView(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("view") String view) {

    Id.Stream.View viewId = Id.Stream.View.from(namespaceId, view);
    streamAdmin.deleteView(viewId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/views/stream/{view}")
  public void showView(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("view") String view) {

    Id.Stream.View viewId = Id.Stream.View.from(namespaceId, view);
    StreamViewProperties properties = streamAdmin.getView(viewId);
    responder.sendJson(HttpResponseStatus.OK, properties, StreamViewProperties.class, GSON);
  }

  @GET
  @Path("/views/stream")
  public void listViews(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId) {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    List<StreamViewProperties> views = streamAdmin.listViews(namespace);
    responder.sendJson(HttpResponseStatus.OK, views, new TypeToken<List<StreamViewProperties>>() { }.getType(), GSON);
  }

  private void checkStreamExists(Id.Stream streamId) throws Exception {
    if (!streamAdmin.exists(streamId)) {
      throw new NotFoundException(streamId);
    }
  }

  /**
   * Gets stream view properties from the request.
   * If the request is invalid, {@link BadRequestException} will be thrown.
   */
  private StreamViewProperties getAndValidateViewConfig(Reader reader) throws BadRequestException {
    StreamViewProperties properties;
    try {
      properties = GSON.fromJson(reader, StreamViewProperties.class);
    } catch (Exception e) {
      throw new BadRequestException("Invalid stream view configuration. Please check that the " +
                                      "configuration is a valid JSON Object with a valid schema.");
    }

    return Validator.validate(properties);
  }
}
