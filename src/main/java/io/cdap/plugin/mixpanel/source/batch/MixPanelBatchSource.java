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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Plugin that reads event from MixPanel api.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MixPanelBatchSource.NAME)
@Description("Reads events from MixPanel.")
public class MixPanelBatchSource extends BatchSource<NullWritable, Text, StructuredRecord> {
  public static final String NAME = "MixPanel";
  private static final Schema MIX_PANEL_RECORD_SCHEMA = Schema.recordOf(
    "mixPanelRecord", Schema.Field.of("event", Schema.of(Schema.Type.STRING))
  );

  private final MixPanelBatchSourceConfig config;

  public MixPanelBatchSource(MixPanelBatchSourceConfig config) {
    this.config = config;

  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    validateConfiguration(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(MIX_PANEL_RECORD_SCHEMA);
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) {
    validateConfiguration(batchSourceContext.getFailureCollector());
    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.referenceName);
    lineageRecorder.createExternalDataset(MIX_PANEL_RECORD_SCHEMA);
    lineageRecorder.recordRead("Read", "Reading MixPanel events",
                               Objects.requireNonNull(MIX_PANEL_RECORD_SCHEMA.getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    batchSourceContext.setInput(Input.of(config.referenceName, new MixPanelInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<NullWritable, Text> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(StructuredRecord.builder(MIX_PANEL_RECORD_SCHEMA).set("event", input.getValue().toString()).build());
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(config.referenceName, failureCollector);
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
