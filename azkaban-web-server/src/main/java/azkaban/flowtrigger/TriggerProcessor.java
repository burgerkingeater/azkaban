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
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
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
  private final ProjectManager projectManager;
  private final ExecutorManager executorManager;

  @Inject
  public TriggerProcessor(final ProjectManager projectManager,
      final ExecutorManager executorManager) {
    Preconditions.checkNotNull(projectManager);
    Preconditions.checkNotNull(executorManager);
    this.projectManager = projectManager;
    this.executorManager = executorManager;
  }

  private void processSucceed(final TriggerInstance triggerInst) {
    logger.debug("process succeed for " + triggerInst);
    // email and trigger a new flow
    final Project project = FlowUtils.getProject(this.projectManager, triggerInst.getProjectId());
    final Flow flow = FlowUtils.getFlow(project, triggerInst.getFlowName());
    final ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(project, flow);
    try {
      this.executorManager.submitExecutableFlow(executableFlow, triggerInst.getSubmitUser());
    } catch (final ExecutorManagerException ex) {
      logger.error(ex.getMessage());
    }
  }

  private void processKilled(final TriggerInstance triggerInst) {
    logger.debug("process killed for " + triggerInst);
    // email
  }

  private void processTimeout(final TriggerInstance triggerInst) {
    logger.debug("process timeout for " + triggerInst);
    // email
  }

  private void processNewInstance(final TriggerInstance triggerInst) {
    logger.debug("process new instance for " + triggerInst);
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      //insert depinst to db
    }
  }

  public void processStatusUpdate(final TriggerInstance updatedTriggerInst) {
    switch (updatedTriggerInst.getStatus()) {
      case RUNNING:
        processNewInstance(updatedTriggerInst);
        break;
      case SUCCEEDED:
        processSucceed(updatedTriggerInst);
        break;
      case KILLED:
        processKilled(updatedTriggerInst);
        break;
      case TIMEOUT:
        processTimeout(updatedTriggerInst);
        break;
      default:
        break;
    }
  }
}
