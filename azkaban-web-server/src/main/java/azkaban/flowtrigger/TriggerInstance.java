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

import azkaban.project.FlowConfigID;
import azkaban.project.FlowTrigger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class TriggerInstance {

  private final List<DependencyInstance> depInstances;
  private final String id;
  private final FlowConfigID flowConfigID;
  private final String submitUser;
  private FlowTrigger flowTrigger;
  private volatile int flowExecId; // associated flow execution id

  //todo chengren311: convert it to builder
  public TriggerInstance(final String id, final FlowTrigger flowTrigger, final FlowConfigID
      flowConfigID, final String submitUser, final List<DependencyInstance> depInstances,
      final int flowExecId) {
    Preconditions.checkNotNull(flowConfigID);
    this.depInstances = ImmutableList.copyOf(depInstances);
    this.id = id;
    this.flowTrigger = flowTrigger;
    this.submitUser = submitUser;
    this.flowConfigID = flowConfigID;
    this.flowExecId = flowExecId;
    for (final DependencyInstance depInst : this.depInstances) {
      depInst.setTriggerInstance(this);
    }
  }

  public static void main(final String[] args) throws InterruptedException {
//    final DependencyInstance di1 = new DependencyInstance(null, null, null, null, Status
//        .KILLED, KillingCause.MANUAL);
//
//    final DependencyInstance di2 = new DependencyInstance(null, null, null, null, Status
//        .TIMEOUT, KillingCause.MANUAL);
//
//    final DependencyInstance di3 = new DependencyInstance(null, null, null, null, Status
//        .SUCCEEDED, KillingCause.MANUAL);
//
//    final DependencyInstance di4 = new DependencyInstance(null, null, null, null, Status
//        .SUCCEEDED, KillingCause.MANUAL);
//
//    final DependencyInstance di5 = new DependencyInstance(null, null, null, null, Status
//        .SUCCEEDED, KillingCause.MANUAL);
//
//    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
//
//    dependencyInstanceList.add(di1);
//    dependencyInstanceList.add(di2);
//    dependencyInstanceList.add(di3);
//    dependencyInstanceList.add(di4);
//    dependencyInstanceList.add(di5);
//
//    final TriggerInstance ti = new TriggerInstance("1", null,
//        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
//
//    System.out.println(ti.getStatus());
  }


  public FlowConfigID getFlowConfigID() {
    return this.flowConfigID;
  }

  public int getFlowExecId() {
    return this.flowExecId;
  }

  public void setFlowExecId(final int flowExecId) {
    this.flowExecId = flowExecId;
  }

  public final FlowTrigger getFlowTrigger() {
    return this.flowTrigger;
  }

  public void setFlowTrigger(final FlowTrigger flowTrigger) {
    this.flowTrigger = flowTrigger;
  }

  public String getSubmitUser() {
    return this.submitUser;
  }

  public void addDependencyInstance(final DependencyInstance depInst) {
    this.depInstances.add(depInst);
  }

  public List<DependencyInstance> getDepInstances() {
    return this.depInstances;
  }

  public String getId() {
    return this.id;
  }

  private boolean isRunning(final Set<Status> statuses) {
    // 1. all dependencies are running or 2. at least one is running and rest are succeeded
//    return (statusCount.containsKey(Status.RUNNING) && statusCount.size() == 1) ||
//        (statusCount.containsKey(Status.RUNNING) && statusCount.containsKey(Status.SUCCEEDED) &&
//            statusCount.size() == 2);

    if (statuses.contains(Status.RUNNING)) {
      for (final Status status : statuses) {
        if (!status.equals(Status.SUCCEEDED) && !status.equals(Status.RUNNING)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean isSucceed(final Set<Status> statuses) {
    // 1. all dependencies are succeeded
    return statuses.contains(Status.SUCCEEDED) && statuses.size() == 1;
  }


  private boolean isTimeout(final Set<Status> statuses) {
    if (statuses.contains(Status.TIMEOUT)) {
      for (final Status status : statuses) {
        if (!Status.isDone(status) || status.equals(Status.FAILED)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean isKilled(final Set<Status> statuses) {
//    // 1. all dependencies are killed 2. at least one is killed and rest are succeeded
//    return (statusCount.containsKey(Status.KILLED) && statusCount.size() == 1) || (statusCount
//        .containsKey(Status.KILLED) && statusCount.containsKey(Status.SUCCEEDED) && statusCount
//        .size() == 2);

    if (statuses.contains(Status.KILLED)) {
      for (final Status status : statuses) {
        if (!status.equals(Status.SUCCEEDED) && !status.equals(Status.KILLED)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean isFailed(final Set<Status> statuses) {
    // 1. any of the dependency instance is failed and rest, if any, are succeeded/killed/timeout.
    // failed status overrides other terminal status, which means even one dependency
    // instance is failed and rest are succeeded/killed/timeout, the trigger instance is
    // considered as failed. This is to alert users of investigating the failure even if other
    // dependency instances are killed/timeout.
    if (statuses.contains(Status.FAILED)) {
      for (final Status status : statuses) {
        if (!Status.isDone(status)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public Status getStatus() {
    // no-dependency trigger is always considered as success
    if (this.depInstances.isEmpty()) {
      return Status.SUCCEEDED;
    }
    final Set<Status> statusSet = new HashSet<>();

    for (final DependencyInstance depInst : this.depInstances) {
      statusSet.add(depInst.getStatus());
    }

    if (isRunning(statusSet)) {
      return Status.RUNNING;
    } else if (isSucceed(statusSet)) {
      return Status.SUCCEEDED;
    } else if (isFailed(statusSet)) {
      return Status.FAILED;
    } else if (isTimeout(statusSet)) {
      return Status.TIMEOUT;
    } else if (isKilled(statusSet)) {
      return Status.KILLED;
    } else {
      return Status.KILLING;
    }
  }

  public KillingCause getKillingCause() {
    final Set<KillingCause> killingCauses = this.depInstances.stream()
        .map(DependencyInstance::getKillingCause).collect(Collectors.toSet());
    if (killingCauses.contains(KillingCause.FAILURE)) {
      return KillingCause.FAILURE;
    } else if (killingCauses.contains(KillingCause.TIMEOUT)) {
      return KillingCause.TIMEOUT;
    } else if (killingCauses.contains(KillingCause.MANUAL)) {
      return KillingCause.MANUAL;
    } else {
      return KillingCause.NONE;
    }
  }

  public Date getStartTime() {
    final List<Date> startTimeList = this.depInstances.stream()
        .map(DependencyInstance::getStartTime).collect(Collectors.toList());
    return startTimeList.isEmpty() ? null : Collections.min(startTimeList);
  }

  public Date getEndTime() {
    if (Status.isDone(this.getStatus())) {
      final List<Date> endTimeList = this.depInstances.stream()
          .map(DependencyInstance::getEndTime).filter(endTime -> endTime != null)
          .collect(Collectors.toList());
      return endTimeList.isEmpty() ? null : Collections.max(endTimeList);
    } else {
      return null;
    }
  }
}
