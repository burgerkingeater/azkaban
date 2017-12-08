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
import java.util.Optional;

public class DependencyInstance {

  private final Date startTime;
  // dependency name as defined by user
  private final String depName;
  private TriggerInstance triggerInstance;
  private Optional<DependencyInstanceContext> context;
  private volatile Date endTime;
  private volatile Status status;
  private volatile CancellationCause cause;

//  public DependencyInstance(final String depName, final Date startTime,
//      final DependencyInstanceContext context, final KillingCause cause) {
//    this.depName = depName;
//    this.startTime = startTime;
//    this.context = Optional.ofNullable(context);
//    this.cause = cause;
//    this.status = null;
//    this.endTime = null;
//  }

  //todo chengren311: use builder pattern to construct the object
  public DependencyInstance(final String depName, final Date startTime,
      final Date endTime, final DependencyInstanceContext context, final Status status,
      final CancellationCause cause) {
    this.status = status;
    this.depName = depName;
    this.startTime = startTime;
    this.endTime = endTime;
    this.context = Optional.ofNullable(context);
    this.cause = cause;
  }

  public CancellationCause getCancellationCause() {
    return this.cause;
  }

  public void setCancellationCause(final CancellationCause cancellationCause) {
    this.cause = cancellationCause;
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
    this.context = Optional.ofNullable(context);
  }

  public Date getStartTime() {
    return this.startTime;
  }

  public Date getEndTime() {
    return this.endTime;
  }

  public void setEndTime(final Date endTime) {
    this.endTime = endTime;
  }

  public String getDepName() {
    return this.depName;
  }

  public Optional<DependencyInstanceContext> getContext() {
    return this.context;
  }

  public Status getStatus() {
    return this.status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

}
