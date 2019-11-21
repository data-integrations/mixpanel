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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class to work with MixPanel schema.
 */
public class MixPanelSchemaHelper {
  /**
   * Parsed MixPanel event.
   */
  public static class MixPanelEvent {
    public String event;
    public Map<String, String> properties;
  }

  private static final Gson GSON = new GsonBuilder().create();
  private static final String EVENT_NAME_FIELD = "event_name";
  private static final String EVENT_NAME_FIELD_DESC = "$event_name";
  private static final Schema MIX_PANEL_RECORD_SCHEMA = Schema.recordOf(
    "mixPanelRecord", Schema.Field.of("raw_event", Schema.of(Schema.Type.STRING)));

  public static Schema getSchemaFromConfig(MixPanelBatchSourceConfig config) {
    if (config.schemaByEvents()) {
      MixPanelApi api = new MixPanelApi(config.getApiSecret(), config.getMixPanelRestApiUrl(),
                                        config.getMixPanelDataUrl());

      Map<String, String> mappedFields = new HashMap<>();

      Set<String> fieldNames = config.getEvents().stream()
        .flatMap(eventName -> api.getEventTopFields(eventName).stream())
        .map(fieldName -> {
          String escapedFieldName = escapeFieldName(fieldName);
          // this is not likely to happen in real world, but lets check to ensure that user will not get incorrect data
          // handle cases where we have fields "$$name" and "$name" that will be escaped to "name" and probably will
          // have conflicts
          if (!mappedFields.containsKey(escapedFieldName)) {
            mappedFields.put(escapedFieldName, fieldName);
          } else if (!mappedFields.get(escapedFieldName).equals(fieldName)) {
            throw new IllegalArgumentException(
              String.format("'%s' escaped to '%s', but '%s' was previously escaped to same value", fieldName,
                            escapedFieldName, mappedFields.get(escapedFieldName)));
          }
          return escapedFieldName;
        })
        .collect(Collectors.toSet());

      // make sure default fields available
      fieldNames.add("event_name");
      fieldNames.add("distinct_id");
      fieldNames.add("time");

      List<Schema.Field> fields = fieldNames.stream()
        .map(s -> Schema.Field.of(s, Schema.nullableOf(Schema.of(Schema.Type.STRING))))
        .collect(Collectors.toList());

      return Schema.recordOf("mixPanelRecord", fields);
    } else {
      return MIX_PANEL_RECORD_SCHEMA;
    }
  }

  public static StructuredRecord getRecordForEvent(MixPanelBatchSourceConfig config, String event) {
    if (config.schemaByEvents()) {
      MixPanelEvent parsedEvent = GSON.fromJson(event, MixPanelEvent.class);
      StructuredRecord.Builder builder = StructuredRecord.builder(config.getSchema());
      builder.set(EVENT_NAME_FIELD, parsedEvent.event);
      parsedEvent.properties.forEach((propertyName, propertyValue) -> {
        propertyName = escapeFieldName(propertyName);
        if (config.getSchema().getField(propertyName) != null) {
          builder.set(propertyName, propertyValue);
        }
      });
      return builder.build();
    } else {
      return StructuredRecord.builder(MIX_PANEL_RECORD_SCHEMA).set("raw_event", event).build();
    }
  }

  /**
   * Makes random field name looks more like avro field name.
   *
   * @param fieldName field name
   * @return escaped field name
   */
  protected static String escapeFieldName(String fieldName) {
    if (fieldName.equals(EVENT_NAME_FIELD_DESC)) {
      return EVENT_NAME_FIELD;
    }
    // strip unwanted symbols
    fieldName = fieldName.replaceAll("[^a-zA-Z0-9_]", "_");
    // strip possible leading and trailing _
    fieldName = fieldName.replaceAll("^_+|_+$", "");
    // ensure starts with [A-Za-z_]
    if (Character.isDigit(fieldName.charAt(0))) {
      fieldName = "_" + fieldName;
    }
    return fieldName;
  }
}
