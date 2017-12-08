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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.database.FlowTriggerLoader;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import com.google.common.base.Preconditions;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TriggerProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TriggerProcessor.class);
  private static final int THREAD_POOL_SIZE = 1;
  private final ProjectManager projectManager;
  private final ExecutorManager executorManager;
  //private final ExecutorService executorService;
  private final FlowTriggerLoader dependencyLoader;

  @Inject
  public TriggerProcessor(final ProjectManager projectManager,
      final ExecutorManager executorManager, final FlowTriggerLoader dependencyLoader) {
    Preconditions.checkNotNull(projectManager);
    Preconditions.checkNotNull(executorManager);
    this.projectManager = projectManager;
    this.executorManager = executorManager;
    //this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    this.dependencyLoader = dependencyLoader;
  }

  private void executeFlowAndUpdateExecID(final TriggerInstance triggerInst) {
    try {
      final Project project = FlowUtils.getProject(this.projectManager, triggerInst
          .getFlowConfigID().getProjectId());
      final Flow flow = FlowUtils.getFlow(project, triggerInst.getFlowConfigID().getFlowId());
      final ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow);
      this.executorManager.submitExecutableFlow(executableFlow, triggerInst.getSubmitUser());

      triggerInst.setFlowExecId(executableFlow.getExecutionId());
      this.dependencyLoader.updateAssociatedFlowExecId(triggerInst);

//      this.executorService.submit(() -> this.dependencyLoader.updateAssociatedFlowExecId
//          (triggerInst));
    } catch (final Exception ex) {
      logger.error(ex.getMessage()); //todo chengren311: should we swallow the exception or
      // notify user
    }
  }

  public void processSucceed(final TriggerInstance triggerInst) {
    logger.debug("process succeed for " + triggerInst);
    executeFlowAndUpdateExecID(triggerInst);
    // email and trigger a new flow
//    this.executorService
//        .submit(() -> executeFlowAndUpdateExecID(triggerInst));

  }

  public void processTermination(final TriggerInstance triggerInst) {
    logger.debug("process termination for " + triggerInst);
    //email
  }

  public void processNewInstance(final TriggerInstance triggerInst) {
    logger.debug("process new instance for " + triggerInst);
    this.dependencyLoader.uploadTriggerInstance(triggerInst);
//    this.executorService
//        .submit(() -> this.dependencyLoader.uploadTriggerInstance(triggerInst));
  }
}
