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
      .setSchemaByEvents("off")
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
      .setSchemaByEvents("off")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    invalidFromDate.validate(failureCollector);
    assertFieldValidationError(failureCollector, MixPanelBatchSourceConfig.PROPERTY_FROM_DATE);

    MixPanelBatchSourceConfig invalidToDate = MixPanelBatchSourceConfig.builder()
      .setFromDate("2345-11-11")
      .setToDate("invalid")
      .setReferenceName("testReference")
      .setSchemaByEvents("off")
      .build();

    failureCollector = new MockFailureCollector(MOCK_STAGE);
    invalidToDate.validate(failureCollector);
    assertFieldValidationError(failureCollector, MixPanelBatchSourceConfig.PROPERTY_TO_DATE);
  }

  @Test
  public void testInvalidUrl() {
    MixPanelBatchSourceConfig invalidUrl = MixPanelBatchSourceConfig.builder()
      .setFromDate("1234-11-11")
      .setToDate("2345-11-11")
      .setMixPanelDataUrl("invalid://url")
      .setMixPanelRestApiUrl("invalid://url")
      .setReferenceName("testReference")
      .setSchemaByEvents("off")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    invalidUrl.validate(failureCollector);
    assertFieldValidationError(failureCollector, MixPanelBatchSourceConfig.PROPERTY_URL,
                               MixPanelBatchSourceConfig.PROPERTY_REST_URL);
  }

  void assertFieldValidationError(MockFailureCollector failureCollector, String... properties) {
    Assert.assertEquals(properties.length, failureCollector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = failureCollector.getValidationFailures().stream()
      .flatMap(validationFailure -> validationFailure.getCauses().stream())
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(properties.length, causeList.size());

    propertyLoop:
    for (String property : properties) {
      for (ValidationFailure.Cause cause : causeList) {
        if (cause.getAttribute(CauseAttributes.STAGE_CONFIG).equals(property)) {
          continue propertyLoop;
        }
      }
      Assert.fail("expected failure for " + property);
    }
  }
}
