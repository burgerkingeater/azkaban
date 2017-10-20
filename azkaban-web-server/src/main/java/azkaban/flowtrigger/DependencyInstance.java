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

public class DependencyInstance {

  private final DependencyInstanceContext context;
  private final long startTime;
  private final TriggerInstance triggerInstance;
  private volatile long endTime;
  private volatile Status status;
  private boolean timeoutKilling;

  public DependencyInstance(final DependencyInstanceContext context,
      final TriggerInstance triggerInst) {
    this.status = Status.RUNNING;
    this.startTime = System.currentTimeMillis();
    this.endTime = -1;
    this.context = context;
    this.timeoutKilling = false;
    this.triggerInstance = triggerInst;
  }

  public TriggerInstance getTriggerInstance() {
    return this.triggerInstance;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }


  public boolean isTimeoutKilling() {
    return this.timeoutKilling;
  }

  public void setTimeoutKilling(final boolean timeoutKilling) {
    this.timeoutKilling = timeoutKilling;
  }

  public String getExecId() {
    return this.triggerInstance.getExecId();
  }

  public void updateEndTime(final long endTime) {
    this.endTime = endTime;
  }

  public void updateStatus(final Status status) {
    this.status = status;
  }

  public DependencyInstanceContext getContext() {
    return this.context;
  }

  public Status getStatus() {
    return this.status;
  }
}