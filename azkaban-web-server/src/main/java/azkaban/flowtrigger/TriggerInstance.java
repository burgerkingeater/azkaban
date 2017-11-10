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

import azkaban.Constants;
import azkaban.project.FlowTrigger;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class TriggerInstance {

  private final List<DependencyInstance> depInstances;
  private final String id;

  /*
  private final int projectId;
  private final int projectVersion;
  private final String flowName;*/

  private final FlowTrigger flowTrigger;

  private final String submitUser;
  private volatile int flowExecId; // associated flow execution id

  //todo chengren311: convert it to builder
  public TriggerInstance(final String id, final FlowTrigger flowTrigger, final String submitUser) {
    Preconditions.checkNotNull(flowTrigger);
    this.depInstances = new ArrayList<>();
    this.id = id;
    this.flowTrigger = flowTrigger;
    this.submitUser = submitUser;
    this.flowExecId = Constants.DEFAULT_EXEC_ID;
  }

  public static void main(final String[] args) throws InterruptedException {
    final TriggerInstance ti = new TriggerInstance("1", FlowTriggerUtil.createFlowTrigger(),
        "chren");

    final DependencyInstance di1 = new DependencyInstance(null, null, null);
    di1.updateStatus(Status.KILLED);
    System.out.println(di1.getStartTime());
    ti.addDependencyInstance(di1);

    final DependencyInstance di2 = new DependencyInstance(null, null, null);
    di2.updateStatus(Status.KILLED);
    System.out.println(di2.getStartTime());
    ti.addDependencyInstance(di2);

    Thread.sleep(10 * 1000);
    final DependencyInstance di3 = new DependencyInstance(null, null, null);
    di3.updateStatus(Status.KILLED);
    System.out.println(di3.getStartTime());
    ti.addDependencyInstance(di3);

    final DependencyInstance di4 = new DependencyInstance(null, null, null);
    di4.updateStatus(Status.KILLED);
    di4.updateEndTime(new Date());
    System.out.println(di4.getStartTime());
    ti.addDependencyInstance(di4);

    System.out.println(ti.getStatus());
    System.out.println(ti.getStartTime());
    System.out.println(ti.getEndTime());
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

  private boolean isRunning(final Map<Status, Integer> statusCount) {
    // 1. all dependencies are running or 2. at least one is running and rest are succeeded
    return (statusCount.containsKey(Status.RUNNING) && statusCount.size() == 1) ||
        (statusCount.containsKey(Status.RUNNING) && statusCount.containsKey(Status.SUCCEEDED) &&
            statusCount.size() == 2);
  }

  private boolean isSucceed(final Map<Status, Integer> statusCount) {
    return statusCount.containsKey(Status.SUCCEEDED) && statusCount.size() == 1;
  }

  private boolean isTimeout(final Map<Status, Integer> statusCount) {
    return (statusCount.containsKey(Status.TIMEOUT) && statusCount.size() == 1) || (statusCount
        .containsKey(Status.TIMEOUT) && statusCount.containsKey(Status.SUCCEEDED) && statusCount
        .size() == 2);
  }

  private boolean isKilled(final Map<Status, Integer> statusCount) {
    return (statusCount.containsKey(Status.KILLED) && statusCount.size() == 1) || (statusCount
        .containsKey(Status.KILLED) && statusCount.containsKey(Status.SUCCEEDED) && statusCount
        .size() == 2);
  }

  public Status getStatus() {
    // no-dependency trigger is always considered as success
    if (this.depInstances.isEmpty()) {
      return Status.SUCCEEDED;
    }
    final Map<Status, Integer> statusCount = new HashMap<>();
    for (final DependencyInstance depInst : this.depInstances) {
      final Integer count = statusCount.get(depInst.getStatus());
      statusCount.put(depInst.getStatus(), count == null ? 1 : count + 1);
    }
    if (isRunning(statusCount)) {
      return Status.RUNNING;
    } else if (isSucceed(statusCount)) {
      return Status.SUCCEEDED;
    } else if (isTimeout(statusCount)) {
      return Status.TIMEOUT;
    } else if (isKilled(statusCount)) {
      return Status.KILLED;
    } else {
      return Status.KILLING;
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
