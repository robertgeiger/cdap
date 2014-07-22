/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.reactor.client;

import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.reactor.client.config.ReactorClientConfig;
import com.continuuity.reactor.client.exception.StreamNotFoundException;
import com.continuuity.reactor.client.util.RestClient;
import com.continuuity.reactor.metadata.StreamRecord;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to interact with Reactor streams.
 */
public class ReactorStreamClient {

  private static final Gson GSON = new Gson();

  private final RestClient restClient;
  private final ReactorClientConfig config;

  @Inject
  public ReactorStreamClient(ReactorClientConfig config) {
    this.config = config;
    this.restClient = RestClient.create(config);
  }

  /**
   * Creates a stream.
   *
   * @param newStreamId ID of the new stream to create
   * @throws IOException if a network error occurred
   */
  public void create(String newStreamId) throws IOException {
    URL url = config.resolveURL(String.format("streams/%s", newStreamId));
    restClient.execute(HttpMethod.PUT, url);
  }

  /**
   * Sends an event to a stream.
   *
   * @param streamId ID of the stream
   * @param event event to send to the stream
   * @throws IOException if a network error occurred
   * @throws StreamNotFoundException if the stream with the specified ID was not found
   */
  public void sendEvent(String streamId, String event) throws IOException, StreamNotFoundException {
    URL url = config.resolveURL(String.format("streams/%s", streamId));
    HttpRequest request = HttpRequest.post(url).withBody(event).build();

    HttpResponse response = restClient.execute(request, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new StreamNotFoundException(streamId);
    }
  }

  /**
   * Gets a consumer ID for a stream, for reading via {@link #dequeueEvent(String, String)}.
   *
   * @param streamId ID of the stream
   * @return consumer ID
   * @throws IOException if a network error occurred
   * @throws StreamNotFoundException if the stream with the specified ID was not found
   */
  public String getConsumerId(String streamId) throws IOException, StreamNotFoundException {
    URL url = config.resolveURL(String.format("streams/%s", streamId));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, HttpURLConnection.HTTP_NOT_FOUND);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new StreamNotFoundException(streamId);
    }

    return ObjectResponse.fromJsonBody(response, String.class).getResponseObject();
  }

  /**
   * Dequeues an event from a stream, using the consumer ID from {@link #getConsumerId(String)}.
   *
   * @param streamId ID of the stream
   * @param consumerId the consumer ID from {@link #getConsumerId(String)}
   * @return the stream event that was dequeued
   * @throws IOException if a network error occurred
   * @throws StreamNotFoundException if the stream with the specified ID was not found
   */
  public String dequeueEvent(String streamId, String consumerId) throws IOException, StreamNotFoundException {
    URL url = config.resolveURL(String.format("streams/%s/dequeue", streamId));
    Map<String, String> headers = ImmutableMap.of("X-Continuuity-ConsumerId", consumerId);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, headers, HttpURLConnection.HTTP_NOT_FOUND);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new StreamNotFoundException(streamId);
    }

    return ObjectResponse.fromJsonBody(response, String.class).getResponseObject();
  }

  /**
   * Truncates a stream, deleting all stream events belonging to the stream.
   *
   * @param streamId ID of the stream to truncate
   * @throws IOException if a network error occurred
   * @throws StreamNotFoundException if the stream with the specified name was not found
   */
  public void truncate(String streamId) throws IOException, StreamNotFoundException {
    URL url = config.resolveURL(String.format("streams/%s/truncate", streamId));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, HttpURLConnection.HTTP_NOT_FOUND);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new StreamNotFoundException(streamId);
    }
  }

  /**
   * Sets the Time-to-Live (TTL) of a stream. TTL governs how long stream events are readable.
   *
   * @param streamId ID of the stream
   * @param ttlInSeconds desired TTL, in seconds
   * @throws IOException if a network error occurred
   * @throws StreamNotFoundException if the stream with the specified name was not found
   */
  public void setTTL(String streamId, long ttlInSeconds) throws IOException, StreamNotFoundException {
    URL url = config.resolveURL(String.format("streams/%s/config", streamId));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(ImmutableMap.of("ttl", ttlInSeconds))).build();

    HttpResponse response = restClient.execute(request, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new StreamNotFoundException(streamId);
    }
  }

  /**
   * Lists all streams.
   *
   * @return list of {@link StreamRecord}s
   * @throws IOException if a network error occurred
   */
  public List<StreamRecord> list() throws IOException {
    URL url = config.resolveURL(String.format("streams"));
    HttpResponse response = restClient.execute(HttpMethod.GET, url);
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<StreamRecord>>() { }).getResponseObject();
  }
}
