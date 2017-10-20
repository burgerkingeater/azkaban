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

package azkaban.test;

import azkaban.flowtrigger.DependencyCheck;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyPluginConfig;

public class TestDependencyCheck implements DependencyCheck {

  @Override
  public DependencyInstanceContext run(final DependencyInstanceConfig config, final
  DependencyInstanceCallback callback) {
    System.out.println("running TestDependencyCheck with config:" + config);
    return new TestDependencyInstanceContext(callback);
  }

  @Override
  public void shutdown() {

  }

  @Override
  public void init(final DependencyPluginConfig config) {

  }
}

