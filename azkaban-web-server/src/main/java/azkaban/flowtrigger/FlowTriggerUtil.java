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

import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FlowTriggerUtil {

  public static FlowTrigger createFlowTrigger() {
    final FlowTriggerDependency dep1 = new FlowTriggerDependency("dep1", "test1", new HashMap<>
        ());
    final FlowTriggerDependency dep2 = new FlowTriggerDependency("dep2", "test2", new HashMap<>
        ());
    final List<FlowTriggerDependency> depList = new ArrayList<>();
    depList.add(dep1);
    depList.add(dep2);

    return new FlowTrigger(new CronSchedule("* * * * * ?"), depList, Duration.ofMinutes(10),
        1, 1, "1", new ArrayList<>());
  }

  public static FlowTrigger createRealFlowTrigger() {
    final FlowTriggerDependency dep1 = new FlowTriggerDependency("dep1", "test1", new HashMap<>
        ());
    final FlowTriggerDependency dep2 = new FlowTriggerDependency("dep2", "test2", new HashMap<>
        ());
    final List<FlowTriggerDependency> depList = new ArrayList<>();
    depList.add(dep1);
    depList.add(dep2);

    return new FlowTrigger(new CronSchedule("* * * * * ?"), depList, Duration.ofMinutes(1),
        17, 1, "SLAtest", new ArrayList<>());
  }


}
