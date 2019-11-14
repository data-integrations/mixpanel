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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * RecordReader implementation, which reads events in json format from MixPanel api.
 */
public class MixPanelRecordReader extends RecordReader<NullWritable, Text> {
  private static final Gson GSON = new GsonBuilder().create();
  private String currentEvent;
  private MixPanelApi.RawEventsIterator eventsIterator;

  private List<NameValuePair> getExportParameters(MixPanelBatchSourceConfig config) {
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
    return params;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(MixPanelInputFormatProvider.PROPERTY_CONFIG_JSON);
    MixPanelBatchSourceConfig config = GSON.fromJson(configJson, MixPanelBatchSourceConfig.class);

    MixPanelApi api = new MixPanelApi(config.getApiSecret(), config.getMixPanelRestApiUrl(),
                                      config.getMixPanelDataUrl());
    eventsIterator = api.getRawEvents(getExportParameters(config));
  }

  @Override
  public boolean nextKeyValue() {
    if (eventsIterator.hasNext()) {
      currentEvent = eventsIterator.next();
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
    if (eventsIterator != null) {
      eventsIterator.close();
    }
  }
}
