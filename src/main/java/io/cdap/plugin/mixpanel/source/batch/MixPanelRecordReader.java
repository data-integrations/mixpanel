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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * RecordReader implementation, which reads events in json format from MixPanel api.
 */
public class MixPanelRecordReader extends RecordReader<NullWritable, String> {
  private static final Gson gson = new GsonBuilder().create();
  private String currentEvent;
  private List<String> data = new LinkedList<>();
  private Iterator<String> eventIterator;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(MixPanelInputFormatProvider.PROPERTY_CONFIG_JSON);
    MixPanelBatchSourceConfig config = gson.fromJson(configJson, MixPanelBatchSourceConfig.class);

    URL mixPanelUrl = new URL(config.getMixPanelUrl());
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
      new AuthScope(mixPanelUrl.getHost(), mixPanelUrl.getPort()),
      new UsernamePasswordCredentials(config.getApiSecret(), ""));


    try (CloseableHttpClient httpclient =
           HttpClients.custom().setDefaultCredentialsProvider(credentialsProvider).build()) {
      HttpPost request = new HttpPost(config.getMixPanelUrl());

      List<NameValuePair> params = new LinkedList<>();
      params.add(new BasicNameValuePair("from_date", config.getFromDate()));
      params.add(new BasicNameValuePair("to_date", config.getToDate()));
      String events = config.getEvents();
      if (events != null && !events.isEmpty()) {
        params.add(new BasicNameValuePair("event", events));
      }
      String filter = config.getFilter();
      if (filter != null && !filter.isEmpty()) {
        params.add(new BasicNameValuePair("where", filter));
      }
      request.setEntity(new UrlEncodedFormEntity(params));

      try (CloseableHttpResponse response = httpclient.execute(request)) {
        Scanner scanner = new Scanner(response.getEntity().getContent());
        while (scanner.hasNextLine()) {
          data.add(scanner.nextLine());
        }
      }
    }
    eventIterator = data.iterator();
  }

  @Override
  public boolean nextKeyValue() {
    if (eventIterator.hasNext()) {
      currentEvent = eventIterator.next();
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
  public String getCurrentValue() {
    return currentEvent;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() {

  }
}
