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

import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.project.FlowConfigID;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.scheduler.AbstractQuartzJob;
import javax.inject.Inject;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlowTriggerQuartzJob extends AbstractQuartzJob {

  public static final String DELEGATE_CLASS_NAME = "FlowTriggerQuartzJob";
  public static final String SUBMIT_USER = "SUBMIT_USER";
  public static final String PROJECT = "PROJECT";
  public static final String FLOW_TRIGGER = "FLOW_TRIGGER";
  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerQuartzJob.class);
  private final FlowTriggerService triggerService;

  @Inject
  public FlowTriggerQuartzJob(final FlowTriggerService service) {
    this.triggerService = service;
  }

  @Override
  public void execute(final JobExecutionContext context) {

    final JobDataMap data = context.getMergedJobDataMap();
    final String projectJson = data.getString(PROJECT);
    final Project project = FlowUtils.toProject(projectJson);

    // todo chengren311: remove it
    logger.info("executing...");

    this.triggerService
        .startTrigger((FlowTrigger) data.get(FLOW_TRIGGER), (FlowConfigID)
            data.get(FlowConfigID.class.getName()), data.getString(SUBMIT_USER), project);

    //this.depService = dependencyService;
    /*
    final FlowTriggerQuartzService service = asT(getKey(context, DELEGATE_CLASS_NAME),
        FlowTriggerQuartzService.class);
    service.run();*/
  }
}

