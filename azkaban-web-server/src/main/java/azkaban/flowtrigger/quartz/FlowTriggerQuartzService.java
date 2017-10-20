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

package azkaban.flowtrigger.quartz;

import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceConfigImpl;
import azkaban.flowtrigger.FlowDependencyService;
import azkaban.project.FlowTriggerDependency;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FlowTriggerQuartzService implements Serializable {

  List<FlowTriggerDependency> dependencies;
  Duration maxDuration;
  FlowDependencyService depService;

  //todo chengren311: gucify it
  public FlowTriggerQuartzService(final List<FlowTriggerDependency> dependencyList,
      final Duration maxDuration, final FlowDependencyService dependencyService) {
    this.dependencies = dependencyList;
    this.maxDuration = maxDuration;
    this.depService = dependencyService;
  }

  public DependencyInstanceConfig mapToDependencyInstanceConfig(final Map<String, String> props) {
    return new DependencyInstanceConfigImpl(props);
  }

  public void run() {
    System.out.println("starting the quartz service");
    for (final FlowTriggerDependency dep : this.dependencies) {
      /*
      this.depService.start(dep.getType(), mapToDependencyInstanceConfig(dep.getProps()), this
          .maxDuration);*/
    }
  }
}
