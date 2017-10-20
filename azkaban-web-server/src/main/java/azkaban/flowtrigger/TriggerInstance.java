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
import java.util.List;

public class TriggerInstance {

  private final List<DependencyInstance> depInstances;
  private final String execId;

  public TriggerInstance(final String execId) {
    this.depInstances = new ArrayList<>();
    this.execId = execId;
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

  public Status getStatus() {
    final long startTime = -1;
    final long endTime = Long.MIN_VALUE;
    return null;
  }

  public long getStartTime() {
    return -1;
  }

  public long getEndTime() {
    return -1;
  }
}
