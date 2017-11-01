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

import azkaban.project.FlowTriggerDependency;
import javax.inject.Singleton;

@Singleton
public class FlowTriggerUtil {

  public static DependencyInstanceContext createDepInstContext(
      final FlowTriggerPluginManager triggerManager, final
  FlowTriggerDependency
      dep) {
    final DependencyCheck dependencyCheck = triggerManager.getDependencyCheck(dep.getType());
    final DependencyInstanceCallback callback = new DependencyInstanceCallbackImpl(this);
    final DependencyInstanceConfig config = new DependencyInstanceConfigImpl(dep.getProps());
    return dependencyCheck.run(config, callback);
  }
}
