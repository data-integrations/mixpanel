package io.cdap.plugin.mixpanel.source.batch;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class MixPanelSchemaHelperTest {
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(
    WireMockConfiguration.wireMockConfig().dynamicPort()
  );

  @Test
  public void testEscapeFieldName() {
    Assert.assertEquals("_1_Starts_With_number", MixPanelSchemaHelper.escapeFieldName("1 Starts With number"));
    Assert.assertEquals("os", MixPanelSchemaHelper.escapeFieldName("$os"));
    Assert.assertEquals("mega_field", MixPanelSchemaHelper.escapeFieldName("$$$$$mega_field$$$$"));
  }

  @Test
  public void testGetSchema() throws IOException {
    WireMock.stubFor(
      WireMock.post(WireMock.urlMatching("/api/2.0/events/properties/top/"))
        .withBasicAuth("secret", "")
        .withRequestBody(WireMock.containing("event1"))
        .willReturn(WireMock.aResponse().withBody(TestHelper.getResource("describe conflicting 1.json"))
        )
    );

    MixPanelBatchSourceConfig config = MixPanelBatchSourceConfig.builder()
      .setMixPanelRestApiUrl(String.format("http://localhost:%d/", wireMockRule.port()))
      .setApiSecret("secret")
      .setSchemaByEvents("on")
      .setEvents("event1")
      .build();

    Schema schema = MixPanelSchemaHelper.getSchemaFromConfig(config);
    Set<String> schemaFields = schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toSet());
    Assert.assertEquals(5, schemaFields.size());
    Assert.assertTrue(schemaFields.contains("regular_field"));
    Assert.assertTrue(schemaFields.contains("distinct_id"));
    Assert.assertTrue(schemaFields.contains("time"));
    Assert.assertTrue(schemaFields.contains("event"));
    Assert.assertTrue(schemaFields.contains("conflict"));
  }

  @Test
  public void testGetSchemaFromConfigConflictingFields() throws IOException {
    WireMock.stubFor(
      WireMock.post(WireMock.urlMatching("/api/2.0/events/properties/top/"))
        .withBasicAuth("secret", "")
        .withRequestBody(WireMock.containing("event1"))
        .willReturn(WireMock.aResponse().withBody(TestHelper.getResource("describe conflicting 1.json"))
        )
    );
    WireMock.stubFor(
      WireMock.post(WireMock.urlMatching("/api/2.0/events/properties/top/"))
        .withBasicAuth("secret", "")
        .withRequestBody(WireMock.containing("event2"))
        .willReturn(WireMock.aResponse().withBody(TestHelper.getResource("describe conflicting 2.json"))
        )
    );

    MixPanelBatchSourceConfig config = MixPanelBatchSourceConfig.builder()
      .setMixPanelRestApiUrl(String.format("http://localhost:%d/", wireMockRule.port()))
      .setApiSecret("secret")
      .setSchemaByEvents("on")
      .setEvents("event1,event2")
      .build();

    try {
      MixPanelSchemaHelper.getSchemaFromConfig(config);
      Assert.fail("getting schema for events with conflicting fields should file");
    } catch (IllegalArgumentException ex) {
      Assert.assertEquals(
        "'$$conflict$$$$' escaped to 'conflict', but '$conflict$' was previously escaped to same value",
        ex.getMessage());
    }
  }
}
