/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import org.quartz.Trigger;
import org.quartz.spi.OperableTrigger;

/**
 * Trigger and state.
 * TriggerStateV2 is added in CDAP 3.3 as a new class to wrap the trigger and its state to store them in the store
 * in JSON format.
 */
class TriggerStatusV2 {
  final OperableTrigger trigger;
  final Trigger.TriggerState state;

  TriggerStatusV2(OperableTrigger trigger, Trigger.TriggerState state) {
    this.trigger = trigger;
    this.state = state;
  }
}
