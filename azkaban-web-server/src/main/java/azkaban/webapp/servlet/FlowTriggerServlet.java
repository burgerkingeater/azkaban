/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.webapp.servlet;

import azkaban.flowtrigger.CancellationCause;
import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class FlowTriggerServlet extends LoginAbstractAzkabanServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(FlowTriggerServlet.class);
  private FlowTriggerService triggerService;
  private ProjectManager projectManager;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.triggerService = server.getFlowTriggerService();
    this.projectManager = server.getProjectManager();
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      handlePage(req, resp, session);
    }
  }

  private void handlePage(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) {
    final Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/executionflowtriggerspage.vm");

    page.add("runningTriggers", this.triggerService.getRunningTriggers());
    page.add("recentTriggers", this.triggerService.getRecentlyFinished());

    page.add("vmutils", new ExecutorVMHelper());
    page.render();
  }

  private void handleAJAXAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException,
      IOException {
    final HashMap<String, Object> ret = new HashMap<>();
    final String ajaxName = getParam(req, "ajax");

    if (ajaxName.equals("fetchRunningTriggers")) {
      ajaxFetchRunningTriggerInstances(ret);
    } else if (ajaxName.equals("killRunningTrigger")) {
      if (hasParam(req, "id")) {
        final String triggerInstanceId = getParam(req, "id");
        ajaxKillTriggerInstance(triggerInstanceId);
      } else {
        ret.put("error", "please specify a valid running trigger instance id");
      }
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void ajaxKillTriggerInstance(final String triggerInstanceId) {
    this.triggerService.cancel(triggerInstanceId, CancellationCause.MANUAL);

  }

  private void ajaxFetchRunningTriggerInstances(final HashMap<String, Object> ret) throws
      ServletException {
    final Collection<TriggerInstance> triggerInstanceList = this.triggerService
        .getRunningTriggers();

    final List<HashMap<String, Object>> output = new ArrayList<>();
    ret.put("items", output);

    for (final TriggerInstance triggerInstance : triggerInstanceList) {
      writeTriggerInstancesData(output, triggerInstance);
    }
  }

  private void writeTriggerInstancesData(final List<HashMap<String, Object>> output,
      final TriggerInstance triggerInst) {

    final HashMap<String, Object> data = new HashMap<>();
    data.put("id", triggerInst.getId());
    data.put("starttime", triggerInst.getStartTime());
    data.put("endtime", triggerInst.getEndTime());
    data.put("status", triggerInst.getStatus());
    data.put("flowExecutionId", triggerInst.getFlowExecId());
    data.put("submitUser", triggerInst.getSubmitUser());
    data.put("flowTriggerConfig", triggerInst.getFlowTrigger());
    final List<Map<String, Object>> dependencyOutput = new ArrayList<>();
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      final Map<String, Object> depMap = new HashMap<>();
      depMap.put("dependencyName", depInst.getDepName());
      depMap.put("dependencyStarttime", depInst.getStartTime());
      depMap.put("dependencyEndtime", depInst.getEndTime());
      depMap.put("dependencyStatus", depInst.getStatus());
      depMap.put("dependencyConfig", depInst.getTriggerInstance().getFlowTrigger()
          .getDependencyByName
              (depInst.getDepName()));
      dependencyOutput.add(depMap);
    }
    data.put("dependencies", dependencyOutput);
    output.add(data);
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    /*
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else {
      final HashMap<String, Object> ret = new HashMap<>();
      if (hasParam(req, "action")) {
        final String action = getParam(req, "action");
        if (action.equals("scheduleFlow")) {
          ajaxScheduleFlow(req, ret, session.getUser());
        } else if (action.equals("scheduleCronFlow")) {
          ajaxScheduleCronFlow(req, ret, session.getUser());
        } else if (action.equals("removeSched")) {
          ajaxRemoveSched(req, ret, session.getUser());
        }
      }

      if (ret.get("status") == ("success")) {
        setSuccessMessageInCookie(resp, (String) ret.get("message"));
      } else {
        setErrorMessageInCookie(resp, (String) ret.get("message"));
      }

      this.writeJSON(resp, ret);
    }*/
  }

  /**
   * @param cronTimezone represents the timezone from remote API call
   * @return if the string is equal to UTC, we return UTC; otherwise, we always return default
   * timezone.
   */
  private DateTimeZone parseTimeZone(final String cronTimezone) {
    if (cronTimezone != null && cronTimezone.equals("UTC")) {
      return DateTimeZone.UTC;
    }

    return DateTimeZone.getDefault();
  }

  private DateTime getPresentTimeByTimezone(final DateTimeZone timezone) {
    return new DateTime(timezone);
  }

  public class ExecutorVMHelper {

    public String getProjectName(final int id) {
      final Project project = FlowTriggerServlet.this.projectManager.getProject(id);
      if (project == null) {
        return String.valueOf(id);
      }

      return project.getName();
    }
  }
}
