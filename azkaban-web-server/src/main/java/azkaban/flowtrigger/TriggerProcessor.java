/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.flowtrigger;

import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TriggerProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TriggerProcessor.class);

  private void processSucceed(final TriggerInstance triggerInst) {
    logger.debug("process succeed for " + triggerInst);
    // email and trigger a new flow
  }

  private void processKilled(final TriggerInstance triggerInst) {
    logger.debug("process killed for " + triggerInst);
    // email
  }

  private void processTimeout(final TriggerInstance triggerInst) {
    logger.debug("process timeout for " + triggerInst);
    // email
  }

  private void processNewInstance(final TriggerInstance triggerInst) {
    logger.debug("process new instance for " + triggerInst);
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      //insert depinst to db
    }
  }

  public void processStatusUpdate(final TriggerInstance updatedTriggerInst) {
    switch (updatedTriggerInst.getStatus()) {
      case RUNNING:
        processNewInstance(updatedTriggerInst);
        break;
      case SUCCEEDED:
        processSucceed(updatedTriggerInst);
        break;
      case KILLED:
        processKilled(updatedTriggerInst);
        break;
      case TIMEOUT:
        processTimeout(updatedTriggerInst);
        break;
      default:
        break;
    }
  }
}
