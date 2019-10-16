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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.net.MalformedURLException;
import java.net.URL;
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
  public static final String PROPERTY_URL = "mixPanelUrl";

  private static final Pattern DATE_REGEX = Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d");

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
  @Description("Comma separated list of events you would like to get data on.")
  @Nullable
  @Macro
  protected String events;

  @Name(PROPERTY_FILTER)
  @Description("Expression to filter events by.")
  @Nullable
  @Macro
  protected String filter;

  @Name(PROPERTY_URL)
  @Description("MixPanel url.")
  @Nullable
  @Macro
  protected String mixPanelUrl;

  public MixPanelBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  private MixPanelBatchSourceConfig(Builder builder) {
    super(builder.referenceName);
    apiSecret = builder.apiSecret;
    fromDate = builder.fromDate;
    toDate = builder.toDate;
    filter = builder.filter;
    mixPanelUrl = builder.mixPanelUrl;
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

  @Nullable
  public String getEvents() {
    return events;
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  /**
   * Allows to override default mixpanel. Simplifies testing.
   */
  public String getMixPanelUrl() {
    if (mixPanelUrl == null || mixPanelUrl.isEmpty()) {
      return "https://data.mixpanel.com/api/2.0/export/";
    } else {
      return mixPanelUrl;
    }
  }

  void validate(FailureCollector failureCollector) {
    try {
      new URL(getMixPanelUrl());
    } catch (MalformedURLException e) {
      failureCollector
        .addFailure(String.format("Invalid URL '%s'", getMixPanelUrl()), "Change MixPanel url to valid")
        .withConfigProperty(PROPERTY_URL);
    }
    if (!DATE_REGEX.matcher(getFromDate()).matches()) {
      failureCollector
        .addFailure(String.format("Invalid date '%s'", getFromDate()), "Change date to YYYY-MM-DD format")
        .withConfigProperty(PROPERTY_FROM_DATE);
    }
    if (!DATE_REGEX.matcher(getToDate()).matches()) {
      failureCollector
        .addFailure(String.format("Invalid date '%s'", getToDate()), "Change date to YYYY-MM-DD format")
        .withConfigProperty(PROPERTY_TO_DATE);
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
    private String mixPanelUrl;

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

    public Builder setMixPanelUrl(String mixPanelUrl) {
      this.mixPanelUrl = mixPanelUrl;
      return this;
    }

    public MixPanelBatchSourceConfig build() {
      return new MixPanelBatchSourceConfig(this);
    }
  }
}
