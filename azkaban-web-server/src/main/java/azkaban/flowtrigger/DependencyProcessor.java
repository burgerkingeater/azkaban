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

import azkaban.flowtrigger.database.FlowTriggerLoader;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DependencyProcessor {

  private static final Logger logger = LoggerFactory.getLogger(DependencyProcessor.class);
  private static final int THREAD_POOL_SIZE = 1;
  private final FlowTriggerLoader dependencyLoader;
  //private final ExecutorService executorService;

  @Inject
  public DependencyProcessor(final FlowTriggerLoader depLoader) {
    this.dependencyLoader = depLoader;
    //this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
  }

  private void updateDepInst(final DependencyInstance depInst) {
    if (Status.isDone(depInst.getStatus())) {
      this.dependencyLoader.updateDependencyStatusAndEndTime(depInst);
    } else if (depInst.getStatus() == Status.KILLING) {
      this.dependencyLoader.updateDependencyStatusAndKillingCause(depInst);
    } else {
      this.dependencyLoader.updateDependencyStatus(depInst);
    }
  }

  public void processStatusUpdate(final DependencyInstance dep) {
    //update db, will do it in a separate threadpool
    logger.debug("process status update for " + dep);
    System.out.println("process status update for " + dep.getTriggerInstance().getId());
    System.out.println("process status update for " + dep.getTriggerInstance().getId());
    updateDepInst(dep);
//    this.executorService
//        .submit(() -> updateDepInst(dep));
  }

}
