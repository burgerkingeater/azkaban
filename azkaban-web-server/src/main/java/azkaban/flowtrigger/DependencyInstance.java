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

import java.util.Date;

public class DependencyInstance {

  private final Date startTime;
  // dependency name as defined by user
  private final String depName;
  private TriggerInstance triggerInstance;
  private DependencyInstanceContext context;
  private volatile Date endTime;
  private volatile Status status;
  private volatile KillingCause killingCause;

  //todo chengren311: use builder pattern to construct the object
  public DependencyInstance(final String depName, final Date startTime,
      final Date endTime, final DependencyInstanceContext context, final Status status,
      final KillingCause
          killingCause) {
    this.status = status;
    this.depName = depName;
    this.startTime = startTime;
    this.endTime = endTime;
    this.context = context;
    this.killingCause = killingCause;
  }

  public KillingCause getKillingCause() {
    return this.killingCause;
  }

  public void setKillingCause(final KillingCause killingCause) {
    this.killingCause = killingCause;
  }

//  public DependencyInstance(final String depName, final Date startTime, final Date endTime,
//      final Status status, final boolean timeoutKilling, final TriggerInstance triggerInst) {
//    this.depName = depName;
//    this.timeoutKilling = timeoutKilling;
//    this.status = status;
//    this.startTime = startTime;
//    this.endTime = endTime;
//    this.context = null;
//    this.timeoutKilling = false;
//    this.triggerInstance = triggerInst;
//  }

  public TriggerInstance getTriggerInstance() {
    return this.triggerInstance;
  }

  public void setTriggerInstance(final TriggerInstance triggerInstance) {
    this.triggerInstance = triggerInstance;
  }

  public void setDependencyInstanceContext(final DependencyInstanceContext context) {
    this.context = context;
  }

  public Date getStartTime() {
    return this.startTime;
  }

  public Date getEndTime() {
    return this.endTime;
  }

  public String getDepName() {
    return this.depName;
  }

  public void updateEndTime(final Date endTime) {
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
