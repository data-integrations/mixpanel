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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Provides all required configuration for reading MixPanel events.
 */
public class MixPanelBatchSourceConfig extends ReferencePluginConfig {
  public static final String PROPERTY_API_SECRET = "apiSecret";
  public static final String PROPERTY_FROM_DATE = "fromDate";
  public static final String PROPERTY_TO_DATE = "toDate";
  public static final String PROPERTY_EVENTS = "events";
  public static final String PROPERTY_FILTER = "filter";
  public static final String PROPERTY_URL = "mixPanelDataUrl";
  public static final String PROPERTY_REST_URL = "mixPanelRestApiUrl";
  public static final String PROPERTY_SCHEMA_BY_EVENTS = "schemaByEvents";

  private static final Pattern DATE_REGEX = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
  public static final String MIXPANEL_DEFAULT_DATA_URL = "https://data.mixpanel.com/api/2.0/export";
  public static final String MIXPANEL_DEFAULT_REST_API_URL = "https://mixpanel.com";

  @Name(PROPERTY_API_SECRET)
  @Description("Mixpanel API secret.")
  @Macro
  protected String apiSecret;

  @Name(PROPERTY_FROM_DATE)
  @Description("Start date for reports data.")
  @Macro
  protected String fromDate;

  @Name(PROPERTY_TO_DATE)
  @Description("End date for reports data.")
  @Macro
  protected String toDate;

  @Name(PROPERTY_EVENTS)
  @Description("Comma separated list of events get data on.")
  @Nullable
  @Macro
  protected String events;

  @Name(PROPERTY_FILTER)
  @Description("Expression to filter events by.")
  @Nullable
  @Macro
  protected String filter;

  @Name(PROPERTY_URL)
  @Description("MixPanel data url.")
  @Nullable
  @Macro
  protected String mixPanelDataUrl;

  @Name(PROPERTY_REST_URL)
  @Description("MixPanel rest api url.")
  @Nullable
  @Macro
  protected String mixPanelRestApiUrl;

  @Name(PROPERTY_SCHEMA_BY_EVENTS)
  @Description("Include all unique field names from selected events to schema.")
  @Macro
  protected String schemaByEvents;


  private transient Schema schema;

  public MixPanelBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  private MixPanelBatchSourceConfig(Builder builder) {
    super(builder.referenceName);
    apiSecret = builder.apiSecret;
    fromDate = builder.fromDate;
    toDate = builder.toDate;
    filter = builder.filter;
    mixPanelDataUrl = builder.mixPanelDataUrl;
    events = builder.events;
    schemaByEvents = builder.schemaByEvents;
    mixPanelRestApiUrl = builder.mixPanelRestApiUrl;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getApiSecret() {
    return apiSecret;
  }

  public String getFromDate() {
    return fromDate;
  }

  public String getToDate() {
    return toDate;
  }

  public List<String> getEvents() {
    if (events != null && !events.isEmpty()) {
      return Arrays.asList(events.split(","));
    }
    return Collections.emptyList();
  }

  public boolean schemaByEvents() {
    return schemaByEvents.equals("on");
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  /**
   * Allows to override default mixpanel. Simplifies testing.
   */
  public String getMixPanelDataUrl() {
    if (mixPanelDataUrl == null || mixPanelDataUrl.isEmpty()) {
      return MIXPANEL_DEFAULT_DATA_URL;
    }
    return mixPanelDataUrl;
  }

  /**
   * Allows to override default mixpanel. Simplifies testing.
   */
  public String getMixPanelRestApiUrl() {
    if (mixPanelRestApiUrl == null || mixPanelRestApiUrl.isEmpty()) {
      return MIXPANEL_DEFAULT_REST_API_URL;
    }
    return mixPanelRestApiUrl;
  }

  public Schema getSchema() {
    if (schema == null) {
      schema = MixPanelSchemaHelper.getSchemaFromConfig(this);
    }
    return schema;
  }

  void validate(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(referenceName, failureCollector);
    try {
      new URL(getMixPanelDataUrl());
    } catch (MalformedURLException e) {
      failureCollector
        .addFailure(String.format("Invalid data URL '%s'.", getMixPanelDataUrl()), "Change MixPanel data url to valid.")
        .withConfigProperty(PROPERTY_URL);
    }
    try {
      new URL(getMixPanelRestApiUrl());
    } catch (MalformedURLException e) {
      failureCollector
        .addFailure(String.format("Invalid rest api URL '%s'.", getMixPanelDataUrl()),
                    "Change MixPanel rest api url to valid.")
        .withConfigProperty(PROPERTY_REST_URL);
    }
    if (!DATE_REGEX.matcher(getFromDate()).matches()) {
      failureCollector
        .addFailure(String.format("Invalid date '%s'.", getFromDate()), "Change date to YYYY-MM-DD format.")
        .withConfigProperty(PROPERTY_FROM_DATE);
    }
    if (!DATE_REGEX.matcher(getToDate()).matches()) {
      failureCollector
        .addFailure(String.format("Invalid date '%s'.", getToDate()), "Change date to YYYY-MM-DD format.")
        .withConfigProperty(PROPERTY_TO_DATE);
    }
    if (schemaByEvents() && getEvents().isEmpty()) {
      failureCollector
        .addFailure("No events specified.", "Specify event names or uncheck schemaByEvents.")
        .withConfigProperty(PROPERTY_SCHEMA_BY_EVENTS);
    }
  }

  /**
   * Builder for creating a {@link MixPanelBatchSourceConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String apiSecret;
    private String fromDate;
    private String toDate;
    private String events;
    private String filter;
    private String mixPanelDataUrl;
    private String mixPanelRestApiUrl;
    private String schemaByEvents;

    private Builder() {

    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setApiSecret(String apiSecret) {
      this.apiSecret = apiSecret;
      return this;
    }

    public Builder setFromDate(String fromDate) {
      this.fromDate = fromDate;
      return this;
    }

    public Builder setToDate(String toDate) {
      this.toDate = toDate;
      return this;
    }

    public Builder setEvents(String events) {
      this.events = events;
      return this;
    }

    public Builder setFilter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder setMixPanelDataUrl(String mixPanelDataUrl) {
      this.mixPanelDataUrl = mixPanelDataUrl;
      return this;
    }

    public Builder setSchemaByEvents(String schemaByEvents) {
      this.schemaByEvents = schemaByEvents;
      return this;
    }

    public Builder setMixPanelRestApiUrl(String mixPanelRestApiUrl) {
      this.mixPanelRestApiUrl = mixPanelRestApiUrl;
      return this;
    }

    public MixPanelBatchSourceConfig build() {
      return new MixPanelBatchSourceConfig(this);
    }
  }
}
