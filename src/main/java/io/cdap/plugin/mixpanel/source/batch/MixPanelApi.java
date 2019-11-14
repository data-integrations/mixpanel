/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.mixpanel.source.batch;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Stream;

/**
 * MixPanel api wrapper.
 */
public class MixPanelApi {
  private static final String TOP_FIELDS_URL = "/api/2.0/events/properties/top/";
  private static final Gson gson = new Gson();

  private String restApiUrl;
  private String dataApiUrl;
  private HttpClientContext httpClientContext;

  public MixPanelApi(String token, String restApiUrl, String dataApiUrl) {
    this.restApiUrl = restApiUrl.replaceAll("\\/$", "");
    this.dataApiUrl = dataApiUrl.replaceAll("\\/$", "");

    AuthCache authCache = new BasicAuthCache();
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    Stream.of(restApiUrl, dataApiUrl)
      .filter(Objects::nonNull)
      .map(MixPanelApi::uncheckedUrl)
      .forEach(url -> {
                 HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
                 authCache.put(targetHost, new BasicScheme());
                 // MixPanel api auth performed by unique ID as username and empty password. No security issues here
                 // when https used as transport
                 credentialsProvider.setCredentials(
                   new AuthScope(url.getHost(), url.getPort()),
                   new UsernamePasswordCredentials(token, ""));
               }
      );

    httpClientContext = HttpClientContext.create();
    httpClientContext.setCredentialsProvider(credentialsProvider);
    httpClientContext.setAuthCache(authCache);
  }

  public RawEventsIterator getRawEvents(List<NameValuePair> params) {
    try {
      CloseableHttpClient httpClient = HttpClients.createDefault();
      HttpPost request = new HttpPost(this.dataApiUrl);
      request.setEntity(new UrlEncodedFormEntity(params));
      CloseableHttpResponse response = httpClient.execute(request, httpClientContext);
      checkResponseStatus(response, "Failed to fetch raw events ");
      return new RawEventsIterator(httpClient, response);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public Collection<String> getEventTopFields(String eventName) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost request = new HttpPost(restApiUrl + TOP_FIELDS_URL);
      request.setEntity(
        new UrlEncodedFormEntity(Collections.singletonList(new BasicNameValuePair("event", eventName))));

      try (CloseableHttpResponse response = httpClient.execute(request, httpClientContext)) {
        checkResponseStatus(response, String.format("Failed to fetch fields event: '%s', ", eventName));

        String responseContent = IOUtils.toString(response.getEntity().getContent());
        Map<String, Object> result = gson.fromJson(responseContent,
                                                   new TypeToken<Map<String, Object>>() {
                                                   }.getType());
        return result.keySet();
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Iterates over raw events.
   */
  public static class RawEventsIterator implements Iterator<String>, Closeable {
    private Scanner dataScanner;
    private CloseableHttpClient httpClient;
    private CloseableHttpResponse response;

    private RawEventsIterator(CloseableHttpClient httpClient, CloseableHttpResponse response) throws IOException {
      Objects.requireNonNull(httpClient);
      Objects.requireNonNull(response);
      this.httpClient = httpClient;
      this.response = response;
      dataScanner = new Scanner(response.getEntity().getContent());
    }

    @Override
    public boolean hasNext() {
      return dataScanner.hasNextLine();
    }

    @Override
    public String next() {
      return dataScanner.nextLine();
    }

    @Override
    public void close() throws IOException {
      dataScanner.close();
      response.close();
      httpClient.close();
    }
  }

  private void checkResponseStatus(HttpResponse response, String errorMessage) throws IOException {
    if (response.getStatusLine().getStatusCode() >= 300) {
      String output = IOUtils.toString(response.getEntity().getContent(), Charsets.UTF_8);
      throw new IOException(
        String.format(
          "%s code: %s, output: %s",
          errorMessage,
          response.getStatusLine().getStatusCode(),
          output
        )
      );
    }
  }

  private static URL uncheckedUrl(String url) {
    try {
      return new URL(url);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
}
