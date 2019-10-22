package io.cdap.plugin.mixpanel.source.batch;


import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class MixPanelBatchSourceConfigTest {
  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void testValidate() {
    MixPanelBatchSourceConfig config = MixPanelBatchSourceConfig.builder()
      .setFromDate("1234-11-11")
      .setToDate("2345-11-11")
      .setReferenceName("testReference")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testDateInvalid() {
    MixPanelBatchSourceConfig invalidFromDate = MixPanelBatchSourceConfig.builder()
      .setFromDate("invalid")
      .setToDate("2345-11-11")
      .setReferenceName("testReference")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    invalidFromDate.validate(failureCollector);
    assertSingleDateValidationFailed(failureCollector, MixPanelBatchSourceConfig.PROPERTY_FROM_DATE);

    MixPanelBatchSourceConfig invalidToDate = MixPanelBatchSourceConfig.builder()
      .setFromDate("2345-11-11")
      .setToDate("invalid")
      .setReferenceName("testReference")
      .build();

    failureCollector = new MockFailureCollector(MOCK_STAGE);
    invalidToDate.validate(failureCollector);
    assertSingleDateValidationFailed(failureCollector, MixPanelBatchSourceConfig.PROPERTY_TO_DATE);
  }

  @Test
  public void testInvalidUrl() {
    MixPanelBatchSourceConfig invalidUrl = MixPanelBatchSourceConfig.builder()
      .setFromDate("1234-11-11")
      .setToDate("2345-11-11")
      .setMixPanelUrl("invalid://url")
      .setReferenceName("testReference")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    invalidUrl.validate(failureCollector);
    assertSingleDateValidationFailed(failureCollector, MixPanelBatchSourceConfig.PROPERTY_URL);
  }

  void assertSingleDateValidationFailed(MockFailureCollector failureCollector, String paramName) {
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = failureCollector.getValidationFailures().get(0).getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(paramName, causeList.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
