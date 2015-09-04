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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreTableManager;
import co.cask.cdap.proto.internal.CreateTableRequest;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements table management for streams and datasets.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/data/explore")
public class ExploreTableHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreTableHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final ExploreTableManager exploreTableManager;

  @Inject
  public ExploreTableHandler(ExploreTableManager exploreTableManager) {
    this.exploreTableManager = exploreTableManager;
  }

  @POST
  @Path("tables/{table}")
  public void createTable(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("table") String table)
    throws IOException, ExploreException, SQLException, BadRequestException {

    // TODO: only allow internal usage
    Id.Table tableId = Id.Table.from(namespace, table);
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()))) {
      CreateTableRequest createTableRequest = GSON.fromJson(reader, CreateTableRequest.class);
      QueryHandle handle = exploreTableManager.createTable(tableId, createTableRequest);
      responder.sendJson(HttpResponseStatus.OK, handle);
    } catch (UnsupportedTypeException e) {
      LOG.warn("Invalid schema provided when creating table {}", table, e);
      throw new BadRequestException(e);
    }
  }

  @DELETE
  @Path("tables/{table}")
  public void deleteTable(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("table") String table) throws ExploreException, SQLException {
    // TODO: only allow internal usage
    Id.Table tableId = Id.Table.from(namespace, table);
    QueryHandle handle = exploreTableManager.deleteTable(tableId);
    responder.sendJson(HttpResponseStatus.OK, handle);
  }

}
