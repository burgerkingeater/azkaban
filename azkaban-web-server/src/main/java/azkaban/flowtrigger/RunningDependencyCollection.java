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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.inject.Singleton;

@Singleton
/**
 * Holds all dependency instances which are in running/killing state.
 * This class is NOT thread safe, it's user's responsibility to guarantee the thread safety.
 */
public class RunningDependencyCollection implements Serializable {

  private final List<DependencyInstance> runningDependencies;

  public RunningDependencyCollection() {
    this.runningDependencies = new ArrayList<>();
  }

  public void add(final DependencyInstance dep) {
    this.runningDependencies.add(dep);
  }

  public DependencyInstance get(final String execId) {
    for (final DependencyInstance dependencyInstance : this.runningDependencies) {
      if (dependencyInstance.getExecId() == execId) {
        return dependencyInstance;
      }
    }
    return null;
  }

  public DependencyInstance get(final DependencyInstanceContext context) {
    for (final DependencyInstance depInst : this.runningDependencies) {
      if (depInst.getContext() == context) {
        return depInst;
      }
    }
    return null;
  }

  public void remove(final DependencyInstanceContext context) {
    final Iterator<DependencyInstance> iter = this.runningDependencies.iterator();
    while (iter.hasNext()) {
      if (iter.next().getContext() == context) {
        iter.remove();
      }
    }
  }

  @Override
  /**
   * @throws ConcurrentModificationException while removing an instance from the list
   */
  public String toString() {
    return "RunningDependencyCollection{" +
        "runningDependencies=" + this.runningDependencies +
        '}';
  }
}
