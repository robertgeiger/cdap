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

package co.cask.cdap.explore.executor;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;

/**
 * TODO: Move to a common package
 */
public abstract class HandlerTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private NettyHttpService httpService;
  private List<? extends Service> userServices;
  private URI baseURI;

  protected abstract Iterable<? extends HttpHandler> getHandlers();
  protected Iterable<? extends Service> getServices() {
    return ImmutableList.of();
  }

  @Before
  public void handlerTestBaseSetUp() {
    userServices = ImmutableList.copyOf(getServices());
    for (Service userService : userServices) {
      userService.startAndWait();
    }

    httpService = NettyHttpService.builder()
      .addHttpHandlers(getHandlers())
      .setExceptionHandler(new HttpExceptionHandler())
      .build();
    httpService.startAndWait();

    InetSocketAddress bindAddress = httpService.getBindAddress();
    baseURI = URI.create("http://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  @After
  public void handlerTestBaseTearDown() {
    httpService.stopAndWait();
    for (Service userService : userServices) {
      userService.stopAndWait();
    }
  }

  protected URL resolve(String format, Object... args) throws MalformedURLException {
    return baseURI.resolve(String.format(format, args)).toURL();
  }

  protected <T> void execute(HttpRequest request, Matcher<Object> expectedCode,
                             Matcher<T> expectedBody, Type bodyType) throws IOException {

    HttpResponse response = HttpRequests.execute(request);
    Assert.assertTrue(
      String.format("Expected response code '%s' but got '%s'", expectedCode, response.getResponseCode()),
      expectedCode.matches(response.getResponseCode()));

    if (!expectedBody.equals(Matchers.any())) {
      try {
        T object = ObjectResponse.<T>fromJsonBody(response, bodyType, GSON).getResponseObject();
        Assert.assertTrue(
          String.format("Expected response body '%s' but got '%s'", expectedBody, object),
          expectedBody.matches(object));
      } catch (JsonSyntaxException e) {
        throw new RuntimeException("Couldn't decode body as JSON: " + response.getResponseBodyAsString(), e);
      }
    }
  }

  protected void execute(HttpRequest request, Matcher<Object> expectedCode) throws IOException {
    execute(request, expectedCode, Matchers.any(), null);
  }
}
