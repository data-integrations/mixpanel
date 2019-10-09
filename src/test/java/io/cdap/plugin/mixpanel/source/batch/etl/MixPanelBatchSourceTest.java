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

package io.cdap.plugin.mixpanel.source.batch.etl;

import com.github.tomakehurst.wiremock.client.BasicCredentials;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.mixpanel.source.batch.MixPanelBatchSource;
import io.cdap.plugin.mixpanel.source.batch.MixPanelBatchSourceConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class MixPanelBatchSourceTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(
    WireMockConfiguration.wireMockConfig().dynamicPort()
  );

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      MixPanelBatchSource.class);
  }

  @Test
  public void testMixPanelSource() throws Exception {
    WireMock.stubFor(
      WireMock.post(
        WireMock.urlMatching("/api/2.0/export"))
        .withBasicAuth("secret", "")
        .willReturn(WireMock.aResponse().withBody("data1\ndata2")
      )
    );

    Map<String, String> mixPanelProperties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "testMixPanelSource")
      .put(MixPanelBatchSourceConfig.PROPERTY_API_SECRET, "secret")
      .put(MixPanelBatchSourceConfig.PROPERTY_FROM_DATE, "2018-10-10")
      .put(MixPanelBatchSourceConfig.PROPERTY_TO_DATE, "2019-10-10")
      .put(MixPanelBatchSourceConfig.PROPERTY_URL,
           String.format("http://localhost:%d/api/2.0/export", wireMockRule.port()))
      .put(MixPanelBatchSourceConfig.PROPERTY_EVENTS, "events")
      .build();

    ETLStage source = new ETLStage("HttpReader", new ETLPlugin(MixPanelBatchSource.NAME, BatchSource.PLUGIN_TYPE,
                                                               mixPanelProperties, null));

    String outputDatasetName = "output-batchsourcetest_mixpanel";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("MixPanelBatchTest");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    WireMock.verify(
      WireMock.postRequestedFor(WireMock.urlEqualTo("/api/2.0/export"))
        .withBasicAuth(new BasicCredentials("secret", ""))
        .withRequestBody(WireMock.containing("from_date=2018-10-10"))
        .withRequestBody(WireMock.containing("to_date=2019-10-10"))
        .withRequestBody(WireMock.containing("event=events"))
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(2, outputRecords.size());
    Assert.assertEquals("data1", outputRecords.get(0).get("event"));
    Assert.assertEquals("data2", outputRecords.get(1).get("event"));
  }
}
