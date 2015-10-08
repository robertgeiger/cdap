/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all client tests.
 */
@Category(XSlowTests.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({
  ApplicationClientTestRun.class,
  ArtifactClientTestRun.class,
  DatasetClientTestRun.class,
  MetaClientTestRun.class,
  MetricsClientTestRun.class,
  MonitorClientTestRun.class,
  NamespaceClientTestRun.class,
  PreferencesClientTestRun.class,
  ProgramClientTestRun.class,
  QueryClientTestRun.class,
  ScheduleClientTestRun.class,
  ServiceClientTestRun.class,
  StreamClientTestRun.class,
  WorkflowClientTestRun.class,
})
public class ClientTestsSuite extends ClientTestBase {

}
