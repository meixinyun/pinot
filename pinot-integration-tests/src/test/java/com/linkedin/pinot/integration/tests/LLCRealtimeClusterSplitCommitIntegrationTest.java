/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import org.apache.commons.configuration.Configuration;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and
 * queries Pinot with split commit enabled from the controller side.
 *
 */
public class LLCRealtimeClusterSplitCommitIntegrationTest extends LLCRealtimeClusterIntegrationTest {

  @Override
  public void startController() {
    startController(ControllerTestUtils.getDefaultControllerConfigurationWithSplitCommit());
  }

  @Override
  public void startServer() {
    Configuration configuration = getDefaultConfiguration();
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
    startServer(configuration);
  }
}
