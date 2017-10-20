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

import com.google.common.base.Preconditions;
import javax.inject.Singleton;

@Singleton
public class TriggerProcessor {

  private final DependencyProcessor depProcessor;

  public TriggerProcessor(final DependencyProcessor depProcessor) {
    this.depProcessor = depProcessor;
  }

  private void processSucceed(final TriggerInstance triggerInst) {
    Preconditions.checkArgument(triggerInst.getStatus() == Status.SUCCEEDED);
    // email and trigger a new flow
  }

  private void processKilled(final TriggerInstance triggerInst) {
    Preconditions.checkArgument(triggerInst.getStatus() == Status.KILLED);
    // email
  }

  private void processKilling(final TriggerInstance triggerInst) {
    Preconditions.checkArgument(triggerInst.getStatus() == Status.KILLING);
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      if (depInst.getStatus() == Status.RUNNING) {
        this.depProcessor.processStatusUpdate(depInst, Status.KILLING);
      }
    }
  }

  private void processTimeout(final TriggerInstance triggerInst) {
    Preconditions.checkArgument(triggerInst.getStatus() == Status.TIMEOUT);
    // email
  }

  public void processNewInstance(final TriggerInstance triggerInst) {
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      //insert depinst to db
    }
  }

  public void processStatusUpdate(final TriggerInstance triggerInst, final Status status) {
    switch (status) {
      case SUCCEEDED:
        processSucceed(triggerInst);
        break;
      case KILLING:
        processKilling(triggerInst);
        break;
      case KILLED:
        processKilled(triggerInst);
        break;
      case TIMEOUT:
        processTimeout(triggerInst);
        break;
      default:
        break;
    }
  }
}
