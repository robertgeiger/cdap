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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.MetaDataInfo;
import co.cask.cdap.explore.service.TableNotFoundException;
import co.cask.cdap.explore.utils.ColumnsArgs;
import co.cask.cdap.explore.utils.FunctionsArgs;
import co.cask.cdap.explore.utils.SchemasArgs;
import co.cask.cdap.explore.utils.TablesArgs;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements explore metadata APIs.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/data/explore")
public class ExploreMetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreMetadataHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final ExploreService exploreService;

  @Inject
  public ExploreMetadataHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @GET
  @Path("/namespaces/{namespace}/data/explore/tables")
  public void getTables(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespaceId) {
    LOG.trace("Received get tables for current user");
    try {
      responder.sendJson(HttpResponseStatus.OK, exploreService.getTables(namespaceId));
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @GET
  @Path("/namespaces/{namespace}/data/explore/tables/{table}/info")
  public void getTableSchema(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace") String namespaceId, @PathParam("table") String table) {
    LOG.trace("Received get table info for table {}", table);
    try {
      responder.sendJson(HttpResponseStatus.OK, exploreService.getTableInfo(namespaceId, table));
    } catch (TableNotFoundException e) {
      LOG.error("Could not find table {}", table, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @POST
  @Path("/namespaces/{namespace}/data/explore/jdbc/tables")
  public void getJDBCTables(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        TablesArgs args = decodeArguments(request, TablesArgs.class, new TablesArgs(null, namespaceId, "%", null));
        LOG.trace("Received get tables with params: {}", args.toString());
        return exploreService.getTables(args.getCatalog(), args.getSchemaPattern(),
                                        args.getTableNamePattern(), args.getTableTypes());
      }
    });
  }

  @POST
  @Path("/namespaces/{namespace}/data/explore/jdbc/columns")
  public void getJDBCColumns(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        ColumnsArgs args = decodeArguments(request, ColumnsArgs.class, new ColumnsArgs(null, namespaceId, "%", "%"));
        LOG.trace("Received get columns with params: {}", args.toString());
        return exploreService.getColumns(args.getCatalog(), args.getSchemaPattern(),
                                         args.getTableNamePattern(), args.getColumnNamePattern());
      }
    });
  }

  @POST
  @Path("/namespaces/{namespace}/data/explore/jdbc/schemas")
  public void getJDBCSchemas(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        SchemasArgs args = decodeArguments(request, SchemasArgs.class, new SchemasArgs(null, namespaceId));
        LOG.trace("Received get schemas with params: {}", args.toString());
        return exploreService.getSchemas(args.getCatalog(), args.getSchemaPattern());
      }
    });
  }

  @POST
  @Path("/namespaces/{namespace}/data/explore/jdbc/functions")
  public void getJDBCFunctions(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        FunctionsArgs args = decodeArguments(request, FunctionsArgs.class, new FunctionsArgs(null, namespaceId, "%"));
        LOG.trace("Received get functions with params: {}", args.toString());
        return exploreService.getFunctions(args.getCatalog(), args.getSchemaPattern(),
                                           args.getFunctionNamePattern());
      }
    });
  }

  @POST
  @Path("jdbc/catalogs")
  public void getJDBCCatalogs(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get catalogs query.");
        return exploreService.getCatalogs();
      }
    });
  }

  @GET
  @Path("jdbc/info/{type}")
  public void getJDBCInfo(HttpRequest request, HttpResponder responder, @PathParam("type") final String type) {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<Void>() {
      @Override
      public Void execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get info for {}", type);
        MetaDataInfo.InfoType infoType = MetaDataInfo.InfoType.fromString(type);
        MetaDataInfo metadataInfo = exploreService.getInfo(infoType);
        responder.sendJson(HttpResponseStatus.OK, metadataInfo);
        return null;
      }
    });
  }

  @POST
  @Path("jdbc/tableTypes")
  public void getJDBCTableTypes(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get table types query.");
        return exploreService.getTableTypes();
      }
    });
  }

  @POST
  @Path("jdbc/types")
  public void getJDBCTypes(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get type info query.");
        return exploreService.getTypeInfo();
      }
    });
  }

  // The following 2 endpoints are only for internal use and will be undocumented.
  // They are called by UnderlyingSystemNamespaceAdmin to create/destroy a database in Hive when a namespace in
  // CDAP is created/destroyed.
  // TODO: Consider addings ACLs to these operations.

  @PUT
  @Path("namespaces/{namespace-id}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        return exploreService.createNamespace(Id.Namespace.from(namespaceId));
      }
    });
  }

  @DELETE
  @Path("namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        return exploreService.deleteNamespace(Id.Namespace.from(namespaceId));
      }
    });
  }

  protected void handleResponseEndpointExecution(HttpRequest request, HttpResponder responder,
                                                 final EndpointCoreExecution<QueryHandle> execution) {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<Void>() {
      @Override
      public Void execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        QueryHandle handle = execution.execute(request, responder);
        JsonObject json = new JsonObject();
        json.addProperty("handle", handle.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return null;
      }
    });
  }

  protected void genericEndpointExecution(HttpRequest request, HttpResponder responder,
                                          EndpointCoreExecution<Void> execution) {
    try {
      execution.execute(request, responder);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  protected <T> T decodeArguments(HttpRequest request, Class<T> argsType, T defaultValue) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return defaultValue;
    }
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8)) {
      T args = GSON.fromJson(reader, argsType);
      return (args == null) ? defaultValue : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    }
  }

  /**
   * Represents the core execution of an endpoint.
   */
  protected interface EndpointCoreExecution<T> {
    T execute(HttpRequest request, HttpResponder responder)
      throws IllegalArgumentException, SQLException, ExploreException, IOException;
  }

}
