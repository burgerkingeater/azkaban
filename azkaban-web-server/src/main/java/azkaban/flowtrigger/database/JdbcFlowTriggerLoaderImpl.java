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

package azkaban.flowtrigger.database;

import azkaban.database.AzkabanDataSource;
import azkaban.database.DataSourceUtils;
import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.flowtrigger.DependencyException;
import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.FlowTriggerUtil;
import azkaban.flowtrigger.Status;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.FlowTrigger;
import azkaban.utils.Props;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class JdbcFlowTriggerLoaderImpl implements FlowTriggerLoader {

  private static final Logger logger = LoggerFactory.getLogger(JdbcFlowTriggerLoaderImpl.class);
  private static final String[] DEPENDENCY_EXECUTIONS_COLUMNS = {"trigger_instance_id", "dep_name",
      "starttime", "endtime", "dep_status", "timeout_killing", "project_id", "project_version",
      "flow_id", "flow_exec_id"};

  private static final String[] FLOW_TRIGGERS_COLUMNS = {"project_id", "project_version",
      "flow_id", "trigger_blob"};

  private static final String DEPENDENCY_EXECUTION_TABLE = "dependency_executions";

  private static final String FLOW_TRIGGER_TABLE = "flow_triggers";

  private static final String INSERT_DEPENDENCY = String.format("INSERT INTO %s(%s) VALUES(%s);"
      + "", DEPENDENCY_EXECUTION_TABLE, org.apache.commons.lang.StringUtils.join
      (DEPENDENCY_EXECUTIONS_COLUMNS, ","), String.join(",", Collections.nCopies
      (DEPENDENCY_EXECUTIONS_COLUMNS.length, "?")));

  private static final String UPDATE_DEPENDENCY_STATUS = String.format("UPDATE %s SET dep_status "
      + "= ? WHERE trigger_instance_id = ? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private static final String UPDATE_DEPENDENCY_STATUS_ENDTIME = String.format("UPDATE %s SET "
          + "dep_status = ?, endtime = ? WHERE trigger_instance_id = ? AND dep_name = ?;",
      DEPENDENCY_EXECUTION_TABLE);

  //todo chengren311: avoid scanning the whole table
  private static final String SELECT_ALL_EXECUTIONS =
      String.format("SELECT %s FROM %s ",
          org.apache.commons.lang.StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_ALL_UNFINISHED_EXECUTIONS =
      String
          .format(
              "SELECT %s FROM %s WHERE trigger_instance_id IN (SELECT trigger_instance_id FROM %s WHERE dep_status in "
                  + "(%s))",
              org.apache.commons.lang.StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
              DEPENDENCY_EXECUTION_TABLE, DEPENDENCY_EXECUTION_TABLE,
              Status.RUNNING.ordinal() + "," + Status.KILLING.ordinal());

  private static final String UPDATE_DEPENDENCY_FLOW_EXEC_ID = String.format("UPDATE %s SET "
      + "flow_exec_id "
      + "= ? WHERE trigger_instance_id = ? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_FLOW_TRIGGER = "";

  private static final String INSERT_FLOW_TRIGGER = String.format("INSERT INTO %s(%s) VALUES(%s)",
      FLOW_TRIGGER_TABLE, StringUtils.join(FLOW_TRIGGERS_COLUMNS, ","),
      String.join(",", Collections.nCopies(FLOW_TRIGGERS_COLUMNS.length, "?")));

  private final DatabaseOperator dbOperator;


  @Inject
  public JdbcFlowTriggerLoaderImpl(final DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  public static void main(final String[] args) {
    final Props props = new Props();
    props.put("database.type", "mysql");
    props.put("mysql.port", 3306);
    props.put("mysql.host", "localhost");
    props.put("mysql.database", "azkaban");
    props.put("mysql.user", "root");
    props.put("mysql.password", "");
    props.put("mysql.numconnections", 1000);
    final AzkabanDataSource dataSource = DataSourceUtils.getDataSource(props);
    final QueryRunner queryRunner = new QueryRunner(dataSource);
    final DatabaseOperator databaseOperator = new DatabaseOperator(queryRunner);
    final FlowTriggerLoader depLoader = new JdbcFlowTriggerLoaderImpl(databaseOperator);
    final List<FlowTrigger> flowTriggers = new ArrayList<>();
    flowTriggers.add(FlowTriggerUtil.createRealFlowTrigger());

    final Collection<TriggerInstance> triggerInstances = depLoader
        .loadAllDependencyInstances(flowTriggers, 200);
    System.out.println(triggerInstances);
    /*
    final FlowTrigger flowTrigger = FlowTriggerUtil.createFlowTrigger();
    //depLoader.uploadFlowTrigger(flowTrigger);
    final TriggerInstance triggerInst = new TriggerInstance("1", flowTrigger, "chren");

    final List<DependencyInstance> depInstList = new ArrayList<>();
    depInstList.add(new DependencyInstance("dep1", null, triggerInst));
    depInstList.add(new DependencyInstance("dep2", null, triggerInst));
    depInstList.add(new DependencyInstance("dep3", null, triggerInst));
    for (final DependencyInstance depInst : depInstList) {
      triggerInst.addDependencyInstance(depInst);
    }

    depLoader.uploadTriggerInstance(triggerInst);
    final DependencyInstance depInst = new DependencyInstance("dep1", null, triggerInst);
    depInst.updateStatus(Status.SUCCEEDED);
    depInst.updateEndTime(new Date());
    triggerInst.setFlowExecId(123123);
    depLoader.updateAssociatedFlowExecId(triggerInst);*/
  }

  @Override
  public void uploadFlowTrigger(final FlowTrigger flowTrigger) {
    final Gson gson = new Gson();
    final String jsonStr = gson.toJson(flowTrigger);
    final byte[] jsonBytes = jsonStr.getBytes(Charsets.UTF_8);

    this.executeUpdate(INSERT_FLOW_TRIGGER, flowTrigger.getProjectId(), flowTrigger
        .getProjectVersion(), flowTrigger.getFlowId(), jsonBytes);
  }

 /*
  @Override
  public Collection<TriggerInstance> loadUnfinishedTriggerInstances() {
    return null;
  }*/

  @Override
  public void updateAssociatedFlowExecId(final TriggerInstance triggerInst) {
    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        transOperator
            .update(UPDATE_DEPENDENCY_FLOW_EXEC_ID, triggerInst.getFlowExecId(),
                triggerInst.getId(), depInst.getDepName());
      }
      return null;
    };
    executeTransaction(insertTrigger);
  }

  private void executeUpdate(final String query, final Object... params) {
    try {
      this.dbOperator.update(query, params);
    } catch (final SQLException ex) {
      throw new DependencyException("Query :" + query + " failed.", ex);
    }
  }

  private void executeTransaction(final SQLTransaction<Integer> tran) {
    try {
      this.dbOperator.transaction(tran);
    } catch (final SQLException ex) {
      throw new DependencyException(tran + " failed.", ex);
    }
  }

  @Override
  public void uploadTriggerInstance(final TriggerInstance triggerInst) {
    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        transOperator
            .update(INSERT_DEPENDENCY, triggerInst.getId(), depInst.getDepName(),
                depInst.getStartTime(), depInst.getEndTime(), depInst.getStatus().ordinal(),
                depInst.isTimeoutKilling(),
                triggerInst.getFlowTrigger().getProjectId(),
                triggerInst.getFlowTrigger().getProjectVersion(),
                triggerInst.getFlowTrigger().getFlowId(),
                triggerInst.getFlowExecId());
      }
      return null;
    };

    executeTransaction(insertTrigger);
  }

  @Override
  public void updateDependencyStatus(final DependencyInstance depInst) {
    executeUpdate(UPDATE_DEPENDENCY_STATUS, depInst.getStatus().ordinal(),
        depInst.getTriggerInstance().getId(), depInst.getDepName());
  }

  @Override
  public void updateDependencyStatusAndEndTime(final DependencyInstance depInst) {
    System.out.println("query:" + UPDATE_DEPENDENCY_STATUS_ENDTIME + depInst.getStatus().ordinal()
        + ":" + depInst.getEndTime() + ":" + depInst.getTriggerInstance().getId() + ":" + depInst
        .getDepName());
    executeUpdate(UPDATE_DEPENDENCY_STATUS_ENDTIME, depInst.getStatus().ordinal(),
        depInst.getEndTime(), depInst.getTriggerInstance().getId(), depInst.getDepName());
  }

  /*
  @Override
  public Collection<TriggerInstance> loadUnfinishedDependencyInstances(
      final List<FlowTrigger> flowTriggers) {
    try {
      return this.dbOperator.query(SELECT_ALL_UNFINISHED_EXECUTIONS, new TriggerInstanceHandler
          (flowTriggers));
    } catch (final SQLException ex) {
      throw new DependencyException("Query :" + SELECT_ALL_UNFINISHED_EXECUTIONS + " failed.", ex);
    }
  }*/

  @Override
  public Collection<TriggerInstance> loadAllDependencyInstances(
      final List<FlowTrigger> flowTriggers, final int limit) {
    try {
      return this.dbOperator.query(SELECT_ALL_EXECUTIONS, new TriggerInstanceHandler
          (flowTriggers, limit));
    } catch (final SQLException ex) {
      throw new DependencyException("Query :" + SELECT_ALL_EXECUTIONS + " failed.", ex);
    }
  }

  private static class TriggerInstanceHandler implements
      ResultSetHandler<Collection<TriggerInstance>> {

    private final Map<String, FlowTrigger> flowTriggers;
    private final int limit;

    public TriggerInstanceHandler(final List<FlowTrigger> flowTriggers, final int limit) {
      this.flowTriggers = new HashMap<>();
      for (final FlowTrigger flowTrigger : flowTriggers) {
        this.flowTriggers.put(generateFlowTriggerKey(flowTrigger.getProjectId(), flowTrigger
            .getProjectVersion(), flowTrigger.getFlowId()), flowTrigger);
      }
      this.limit = limit;
    }

    private String generateFlowTriggerKey(final int projId, final int projVersion,
        final String flowId) {
      return projId + "," + projVersion + "," + flowId;
    }

    @Override
    public Collection<TriggerInstance> handle(final ResultSet rs) throws SQLException {
      final Map<String, TriggerInstance> triggerInstMap = new HashMap<>();
      //todo chengren311: get submitUser from another table with projId, projectVersion
      final String submitUser = "test";

      while (rs.next()) {
        final String triggerInstId = rs.getString(1);
        final String depName = rs.getString(2);
        final Date startTime = rs.getTimestamp(3);
        final Date endTime = rs.getTimestamp(4);
        final Status status = Status.values()[rs.getInt(5)];
        final boolean timeoutKilling = rs.getBoolean(6);
        final int projId = rs.getInt(7);
        final int projVersion = rs.getInt(8);
        final String flowId = rs.getString(9);
        final int flowExecId = rs.getInt(10);

        TriggerInstance triggerInst = triggerInstMap.get(triggerInstId);
        if (triggerInst == null) {
          triggerInst = new TriggerInstance(triggerInstId, this.flowTriggers.get(this
              .generateFlowTriggerKey(projId, projVersion, flowId)), submitUser);
          triggerInst.setFlowExecId(flowExecId);
          triggerInstMap.put(triggerInstId, triggerInst);
        }

        final DependencyInstance depInst = new DependencyInstance(depName, startTime, endTime,
            status, timeoutKilling, triggerInst);
        triggerInst.addDependencyInstance(depInst);
      }
      final List<TriggerInstance> triggerInstances = new ArrayList<>(triggerInstMap.values());
      Collections.sort(triggerInstances, (o1, o2) -> {
        if (o1.getStartTime() == null && o2.getStartTime() == null) {
          return 0;
        } else if (o1.getStartTime() != null && o2.getStartTime() != null) {
          if (o1.getStartTime().getTime() == o2.getStartTime().getTime()) {
            return 0;
          } else {
            return o1.getStartTime().getTime() < o2.getStartTime().getTime() ? 1 : -1;
          }
        } else {
          return o1.getStartTime() == null ? -1 : 1;
        }
      });

      return triggerInstances.subList(0, Math.min(triggerInstances.size(), this.limit));
    }
  }
}
