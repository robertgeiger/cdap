/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link HttpServiceResponder} which delegates calls to
 * the HttpServiceResponder's methods to the matching methods for a {@link HttpResponder}.
 * A response is buffered until execute() is called. This allows you to send the correct response upon
 * a transaction failure, and to not always delegating to the user response.
 */
public class DelayedHttpServiceResponder implements HttpServiceResponder {
  private static final Logger LOG = LoggerFactory.getLogger(DelayedHttpServiceResponder.class);

  private final HttpResponder responder;
  private final MetricsContext metricsContext;
  private BufferedResponse bufferedResponse;

  /**
   * Instantiates the class from a {@link HttpResponder}
   *
   * @param responder the responder which will be bound to
   */
  public DelayedHttpServiceResponder(HttpResponder responder, MetricsContext metricsContext) {
    this.responder = responder;
    this.metricsContext = metricsContext;
  }

  DelayedHttpServiceResponder(DelayedHttpServiceResponder other) {
    this.responder = other.responder;
    this.metricsContext = other.metricsContext;
    this.bufferedResponse = other.bufferedResponse;
  }

  @Override
  public void sendJson(Object object) {
    sendJson(HttpResponseStatus.OK.getCode(), object);
  }

  @Override
  public void sendJson(int status, Object object) {
    sendJson(status, object, object.getClass(), new Gson());
  }

  @Override
  public void sendJson(int status, Object object, Type type, Gson gson) {
    doSend(status, Charsets.UTF_8.encode(gson.toJson(object, type)), "application/json", null, false);
  }

  @Override
  public void sendString(String data) {
    sendString(HttpResponseStatus.OK.getCode(), data, Charsets.UTF_8);
  }

  @Override
  public void sendString(int status, String data, Charset charset) {
    doSend(status, charset.encode(data), "text/plain; charset=" + charset.name(), null, false);
  }

  @Override
  public void sendStatus(int status) {
    sendStatus(status, ImmutableMap.<String, String>of());
  }

  @Override
  public void sendStatus(int status, Multimap<String, String> headers) {
    doSend(status, null, null, headers, false);
  }

  @Override
  public void sendStatus(int status, Map<String, String> headers) {
    sendStatus(status, headers.entrySet());
  }

  @Override
  public void sendStatus(int status, Iterable<? extends Map.Entry<String, String>> headers) {
    doSend(status, null, null, createMultimap(headers), false);
  }

  @Override
  public void sendError(int status, String errorMessage) {
    sendString(status, errorMessage, Charsets.UTF_8);
  }

  @Override
  public void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers) {
    doSend(status, content, contentType, headers, true);
  }

  @Override
  public void send(int status, ByteBuffer content, String contentType, Map<String, String> headers) {
    send(status, content, contentType, headers.entrySet());
  }

  @Override
  public void send(int status, ByteBuffer content, String contentType,
                   Iterable<? extends Map.Entry<String, String>> headers) {
    doSend(status, content, contentType, createMultimap(headers), true);
  }

  protected void doSend(int status, ByteBuffer content, String contentType,
                        @Nullable Multimap<String, String> headers, boolean copy) {
    if (bufferedResponse != null) {
      LOG.warn("Multiple calls to one of the 'send*' methods has been made. Only the last response will be sent.");
    }

    ChannelBuffer channelBuffer = null;
    if (content != null) {
      channelBuffer = copy ? ChannelBuffers.copiedBuffer(content) : ChannelBuffers.wrappedBuffer(content);
    }

    bufferedResponse = new BufferedResponse(status, channelBuffer, contentType, headers);
  }

  private <K, V> Multimap<K, V> createMultimap(Iterable<? extends Map.Entry<K, V>> entries) {
    ImmutableMultimap.Builder<K, V> builder = ImmutableMultimap.builder();
    for (Map.Entry<K, V> entry : entries) {
      builder.put(entry);
    }
    return builder.build();
  }

  /**
   * Since calling one of the send methods multiple times logs a warning, upon transaction failures this
   * method is called to allow setting the failure response without an additional warning.
   */
  public void setTransactionFailureResponse(Throwable t) {
    LOG.error("Exception occurred while handling request:", t);
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    ByteBuffer buffer = Charsets.UTF_8.encode("Exception occurred while handling request: "
                                                + Throwables.getRootCause(t).getMessage());

    bufferedResponse = new BufferedResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(),
                                            ChannelBuffers.wrappedBuffer(buffer),
                                            "text/plain; charset=" + Charsets.UTF_8.name(), null);
  }

  /**
   * Same as calling {@link #execute(boolean) execute(true)}.
   */
  public void execute() {
    execute(true);
  }

  /**
   * Calls to other responder methods in this class only cache the response to be sent. The response is actually
   * sent only when this method is called.
   *
   * @param keepAlive {@code true} to keep the connection open; {@code false} otherwise
   */
  public void execute(boolean keepAlive) {
    Preconditions.checkState(bufferedResponse != null,
                             "Can not call execute before one of the other responder methods are called.");

    Multimap<String, String> headers = LinkedListMultimap.create(bufferedResponse.getHeaders());
    headers.put(HttpHeaders.Names.CONNECTION, keepAlive ? HttpHeaders.Values.KEEP_ALIVE : HttpHeaders.Values.CLOSE);

    responder.sendContent(HttpResponseStatus.valueOf(bufferedResponse.getStatus()), bufferedResponse.getChannelBuffer(),
                          bufferedResponse.getContentType(), headers);
    emitMetrics(bufferedResponse.getStatus());
  }

  private void emitMetrics(int status) {
    StringBuilder builder = new StringBuilder(50);
    builder.append("response.");
    if (status < 100) {
      builder.append("unknown");
    } else if (status < 200) {
      builder.append("information");
    } else if (status < 300) {
      builder.append("successful");
    } else if (status < 400) {
      builder.append("redirect");
    } else if (status < 500) {
      builder.append("client.error");
    } else if (status < 600) {
      builder.append("server.error");
    } else {
      builder.append("unknown");
    }
    builder.append(".count");

    metricsContext.increment(builder.toString(), 1);
    metricsContext.increment("requests.count", 1);
  }

  private static final class BufferedResponse {

    private final int status;
    private final ChannelBuffer channelBuffer;
    private final String contentType;
    private final Multimap<String, String> headers;

    private BufferedResponse(int status, ChannelBuffer channelBuffer,
                             String contentType, @Nullable Multimap<String, String> headers) {
      this.status = status;
      this.channelBuffer = channelBuffer;
      this.contentType = contentType;
      this.headers = headers == null ? ImmutableMultimap.<String, String>of() : ImmutableMultimap.copyOf(headers);
    }

    public int getStatus() {
      return status;
    }

    public ChannelBuffer getChannelBuffer() {
      return channelBuffer;
    }

    public String getContentType() {
      return contentType;
    }

    public Multimap<String, String> getHeaders() {
      return headers;
    }
  }
}
