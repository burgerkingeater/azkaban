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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TriggerInstance {

  private final List<DependencyInstance> depInstances;
  private final String execId;

  public TriggerInstance(final String execId) {
    this.depInstances = new ArrayList<>();
    this.execId = execId;
  }

  public static void main(final String[] args) {
    final TriggerInstance ti = new TriggerInstance("1");
    final DependencyInstance di1 = new DependencyInstance(null, null);
    di1.updateStatus(Status.KILLED);
    ti.addDependencyInstance(di1);

    final DependencyInstance di2 = new DependencyInstance(null, null);
    di2.updateStatus(Status.KILLED);
    ti.addDependencyInstance(di2);

    final DependencyInstance di3 = new DependencyInstance(null, null);
    di3.updateStatus(Status.KILLED);
    ti.addDependencyInstance(di3);

    final DependencyInstance di4 = new DependencyInstance(null, null);
    di4.updateStatus(Status.KILLED);
    ti.addDependencyInstance(di4);

    System.out.println(ti.getStatus());
  }

  public void addDependencyInstance(final DependencyInstance depInst) {
    this.depInstances.add(depInst);
  }

  public List<DependencyInstance> getDepInstances() {
    return this.depInstances;
  }

  public String getExecId() {
    return this.execId;
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

  public long getStartTime() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public long getEndTime() {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
