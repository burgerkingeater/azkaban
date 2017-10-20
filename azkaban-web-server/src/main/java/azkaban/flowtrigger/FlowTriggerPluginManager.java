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

import azkaban.test.TestDependencyCheck;
import javax.inject.Singleton;

@Singleton
public class FlowTriggerPluginManager {

  /**
   * return or create a dependency check based on type
   *
   * @return if the dependencyCheck of the same type already exists, return the check,
   * otherwise create a new one and return.
   */
  public DependencyCheck getDependencyCheck(final String type) {
    return new TestDependencyCheck();
  }

}
