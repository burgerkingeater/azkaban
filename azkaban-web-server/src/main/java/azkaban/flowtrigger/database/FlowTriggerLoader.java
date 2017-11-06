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

package azkaban.flowtrigger.database;

import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.FlowTrigger;
import java.util.Collection;
import java.util.List;

public interface FlowTriggerLoader {

  void uploadTriggerInstance(TriggerInstance triggerInstance);

  void updateDependencyStatus(DependencyInstance depInst);

  void updateDependencyStatusAndEndTime(DependencyInstance depInst);

  void uploadFlowTrigger(FlowTrigger flowTrigger);

  //Collection<TriggerInstance> loadUnfinishedTriggerInstances();

  void updateAssociatedFlowExecId(TriggerInstance triggerInst);

  Collection<TriggerInstance> loadAllDependencyInstances(final List<FlowTrigger> flowTriggers,
      int limit);
}
