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
import com.google.gson.GsonBuilder;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.HttpHost;
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * RecordReader implementation, which reads events in json format from MixPanel api.
 */
public class MixPanelRecordReader extends RecordReader<NullWritable, Text> {
  private static final Gson GSON = new GsonBuilder().create();
  private String currentEvent;
  private CloseableHttpClient httpClient;
  private Scanner dataScanner;
  private CloseableHttpResponse response;

  private HttpClientContext createContext(MixPanelBatchSourceConfig config) throws MalformedURLException {
    URL mixPanelUrl = new URL(config.getMixPanelDataUrl());
    HttpHost targetHost = new HttpHost(mixPanelUrl.getHost(), mixPanelUrl.getPort(), mixPanelUrl.getProtocol());
    AuthCache authCache = new BasicAuthCache();
    authCache.put(targetHost, new BasicScheme());

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    // MixPanel api auth performed by unique ID as username and empty password. No security issues here
    // when https used as transport
    credentialsProvider.setCredentials(
      new AuthScope(mixPanelUrl.getHost(), mixPanelUrl.getPort()),
      new UsernamePasswordCredentials(config.getApiSecret(), ""));

    HttpClientContext context = HttpClientContext.create();
    context.setCredentialsProvider(credentialsProvider);
    context.setAuthCache(authCache);
    return context;
  }

  private void fillRequest(MixPanelBatchSourceConfig config, HttpPost request) throws UnsupportedEncodingException {
    List<NameValuePair> params = new LinkedList<>();
    params.add(new BasicNameValuePair("from_date", config.getFromDate()));
    params.add(new BasicNameValuePair("to_date", config.getToDate()));
    List<String> events = config.getEvents();
    if (!events.isEmpty()) {
      params.add(new BasicNameValuePair("event", GSON.toJson(events)));
    }
    String filter = config.getFilter();
    if (filter != null && !filter.isEmpty()) {
      params.add(new BasicNameValuePair("where", filter));
    }
    request.setEntity(new UrlEncodedFormEntity(params));
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(MixPanelInputFormatProvider.PROPERTY_CONFIG_JSON);
    MixPanelBatchSourceConfig config = GSON.fromJson(configJson, MixPanelBatchSourceConfig.class);

    HttpClientContext context = createContext(config);

    httpClient = HttpClients.createDefault();
    HttpPost request = new HttpPost(config.getMixPanelDataUrl());
    fillRequest(config, request);

    response = httpClient.execute(request, context);
    if (response.getStatusLine().getStatusCode() >= 300) {
      String output = IOUtils.toString(response.getEntity().getContent(), Charsets.UTF_8);
      throw new IOException(
        String.format(
          "Failed to fetch MixPanel events, code: %s, output: %s",
          response.getStatusLine().getStatusCode(),
          output
        )
      );
    }
    dataScanner = new Scanner(response.getEntity().getContent());
  }

  @Override
  public boolean nextKeyValue() {
    if (dataScanner.hasNextLine()) {
      currentEvent = dataScanner.nextLine();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public Text getCurrentValue() {
    return new Text(currentEvent);
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (dataScanner != null) {
      dataScanner.close();
    }
    if (response != null) {
      response.close();
    }
    if (httpClient != null) {
      httpClient.close();
    }
  }
}
