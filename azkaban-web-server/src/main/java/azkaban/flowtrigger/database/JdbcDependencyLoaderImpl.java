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
import azkaban.flowtrigger.Status;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.utils.Props;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

private static class TriggerInstanceHandler implements ResultSetHandler<List<TriggerInstance>> {

  /*
trigger_instance_id varchar(64),
dep_name varchar(128),
starttime datetime not null,
endtime datetime,
dep_status tinyint not null,
timeout_killing boolean not null,
project_id int(11) not null,
project_version int(11) not null,
flow_id varchar(128) not null,
  */

  @Override
  public List<TriggerInstance> handle(final ResultSet rs) throws SQLException {
    final Map<String, TriggerInstance> triggerInstMap = new HashMap<>();
    //todo chengren311: get submitUser from another table with projId, projectVersion
    final String submitUser = "test";

    final List<TriggerInstance> res = new ArrayList<>();
    while (!rs.next()) {
      final String triggerInstId = rs.getString(1);
      final String depName = rs.getString(2);
      final Date startTime = rs.getDate(3);
      final Date endTime = rs.getDate(4);
      final Status status = Status.values()[rs.getInt(5)];
      final boolean timeoutKilling = rs.getBoolean(6);
      final int projId = rs.getInt(7);
      final int projVersion = rs.getInt(8);
      final String flowName = rs.getString(9);

      TriggerInstance triggerInst = triggerInstMap.get(triggerInstId);
      if (triggerInst == null) {
        triggerInst = new TriggerInstance(triggerInstId, projId, projVersion, flowName, submitUser);
      } else {
        final DependencyInstance depInst = new DependencyInstance(depName, startTime, endTime,
            status, timeoutKilling, null, triggerInst);
        triggerInst.addDependencyInstance(depInst);
      }

      //final DependencyInstance depInst = new DependencyInstance(execId, projId, )
    }

    return projects;
  }
}

@Singleton
public class JdbcDependencyLoaderImpl implements DependencyLoader {

  private static final Logger logger = LoggerFactory.getLogger(JdbcDependencyLoaderImpl.class);
  private static final String[] DEPENDENCY_EXECUTIONS_COLUMNS = {"trigger_instance_id", "dep_name",
      "starttime", "endtime", "dep_status", "timeout_killing", "project_id", "project_version",
      "flow_id"};

  private static final String DEPENDENCY_EXECUTION_TABLE = "dependency_executions";

  private static final String INSERT_DEPENDENCY = String.format("INSERT INTO %s(%s) VALUES(%s);"
      + "", DEPENDENCY_EXECUTION_TABLE, org.apache.commons.lang.StringUtils.join
      (DEPENDENCY_EXECUTIONS_COLUMNS, ","), String.join(",", Collections.nCopies
      (DEPENDENCY_EXECUTIONS_COLUMNS.length, "?")));

  private static final String UPDATE_DEPENDENCY_STATUS = String.format("UPDATE %s SET dep_status "
      + "= ? WHERE trigger_instance_id = ? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private static final String UPDATE_DEPENDENCY_STATUS_ENDTIME = String.format("UPDATE %s SET "
          + "dep_status = ?, endtime = ? WHERE trigger_instance_id = ? AND dep_name = ?;",
      DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_ALL_UNFINISHED_EXECUTIONS =
      String
          .format(
              "SELECT %s FROM %s WHERE trigger_instance_id IN (SELECT trigger_instance_id FROM %s WHERE dep_status in "
                  + "(%s))",
              org.apache.commons.lang.StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
              DEPENDENCY_EXECUTION_TABLE, DEPENDENCY_EXECUTION_TABLE,
              Status.RUNNING.ordinal() + "," + Status.KILLING.ordinal());

  private final DatabaseOperator dbOperator;

  @Inject
  public JdbcDependencyLoaderImpl(final DatabaseOperator databaseOperator) {
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
    final DependencyLoader depLoader = new JdbcDependencyLoaderImpl(databaseOperator);
    final TriggerInstance triggerInst = new TriggerInstance("1", 1, "flow1", "user1");
    /*
    final List<DependencyInstance> depInstList = new ArrayList<>();
    depInstList.add(new DependencyInstance("dep1", null, triggerInst));
    depInstList.add(new DependencyInstance("dep2", null, triggerInst));
    depInstList.add(new DependencyInstance("dep3", null, triggerInst));*/
    //depLoader.createDependencies(depInstList);
    final DependencyInstance depInst = new DependencyInstance("dep1", null, triggerInst);
    depInst.updateStatus(Status.SUCCEEDED);
    depInst.updateEndTime(new Date());
    depLoader.updateDependencyStatusAndEndTime(depInst);
    depLoader.loadUnfinishedDependencyInstances();
  }

  private void execute(final SQLTransaction<Integer> tran) {
    try {
      this.dbOperator.transaction(tran);
    } catch (final SQLException ex) {
      throw new DependencyException(tran + " failed.", ex);
    }
  }

  @Override
  public void createDependencies(final List<DependencyInstance> depInstList) {

    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : depInstList) {
        transOperator
            .update(INSERT_DEPENDENCY, depInst.getTriggerInstance().getId(), depInst.getDepName(),
                depInst.getStartTime(), depInst.getEndTime(), depInst.getStatus().ordinal(), depInst
                    .getTriggerInstance().getProjectId(), 1,
                // todo chengren311: project_version tbd
                depInst.getTriggerInstance().getFlowName());
      }
      //transOperator.getConnection().commit(); todo chengren311: all or nothing integrity
      return null;
    };

    execute(insertTrigger);
  }

  @Override
  public void updateDependencyStatus(final DependencyInstance depInst) {
    final SQLTransaction<Integer> updateTriggerStatus = transOperator -> {
      transOperator
          .update(UPDATE_DEPENDENCY_STATUS, depInst.getStatus().ordinal(),
              depInst.getTriggerInstance().getId(), depInst.getDepName());
      //transOperator.getConnection().commit(); todo chengren311: all or nothing integrity
      return null;
    };

    execute(updateTriggerStatus);
  }

  @Override
  public void updateDependencyStatusAndEndTime(final DependencyInstance depInst) {
    final SQLTransaction<Integer> updateTriggerStatusAndEndTime = transOperator -> {
      transOperator
          .update(UPDATE_DEPENDENCY_STATUS_ENDTIME, depInst.getStatus().ordinal(),
              depInst.getEndTime(),
              depInst.getTriggerInstance().getId(), depInst.getDepName());
      //transOperator.getConnection().commit(); todo chengren311: all or nothing integrity
      return null;
    };

    execute(updateTriggerStatusAndEndTime);
  }

  @Override
  public List<DependencyInstance> loadUnfinishedDependencyInstances() {
    //this.dbOperator.query();
    System.out.println(SELECT_ALL_UNFINISHED_EXECUTIONS);
    return null;
  }
}
