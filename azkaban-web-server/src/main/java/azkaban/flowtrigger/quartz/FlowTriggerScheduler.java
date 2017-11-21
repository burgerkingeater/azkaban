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

import static java.util.Objects.requireNonNull;

import azkaban.flow.Flow;
import azkaban.project.FlowLoaderUtils;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.scheduler.QuartzJobDescription;
import azkaban.scheduler.QuartzScheduler;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.SchedulerException;

@Singleton
public class FlowTriggerScheduler {

  private final ProjectLoader projectLoader;
  private final QuartzScheduler scheduler;

  @Inject
  public FlowTriggerScheduler(final ProjectLoader projectLoader, final QuartzScheduler scheduler) {
    this.projectLoader = requireNonNull(projectLoader);
    this.scheduler = requireNonNull(scheduler);
  }

  /**
   * Schedule all possible flows in a project
   */
  public void scheduleAll(final Project project, final String submitUser)
      throws SchedulerException {
    //todo chengren311: unschedule old flows
    //todo chengren311: check thread safety of register job
    for (final Flow flow : project.getFlows()) {
      final int latestFlowVersion = this.projectLoader
          .getLatestFlowVersion(flow.getProjectId(), flow
              .getVersion(), flow.getId());
      if (latestFlowVersion > 0) {
        final File flowFile = this.projectLoader
            .getUploadedFlowFile(project.getId(), project.getVersion(),
                latestFlowVersion, flow.getId());
        final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
        if (flowTrigger != null) {
          flowTrigger.setProjectId(project.getId());
          flowTrigger.setProjectVersion(project.getVersion());
          flowTrigger.setFlowId(flow.getId());
          flowTrigger.setFlowVersion(latestFlowVersion);

          final Map<String, Object> contextMap = ImmutableMap.of("submitUser", submitUser,
              "flowTrigger", flowTrigger);
          this.scheduler
              .registerJob(flowTrigger.getSchedule().getCronExpression(), new QuartzJobDescription
                  (FlowTriggerQuartzJob.class, "FlowTriggerQuartzJob", contextMap));
        }
      }
    }
  }


  public void start() {
    this.scheduler.start();
  }

  public void shutdown() {
    this.scheduler.shutdown();
  }

}
