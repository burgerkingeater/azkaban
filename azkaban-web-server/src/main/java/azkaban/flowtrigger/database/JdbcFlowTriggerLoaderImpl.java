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

import azkaban.Constants;
import azkaban.database.AzkabanDataSource;
import azkaban.database.DataSourceUtils;
import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.CancellationCause;
import azkaban.flowtrigger.DependencyException;
import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.Status;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.FlowConfigID;
import azkaban.project.FlowLoaderUtils;
import azkaban.project.FlowTrigger;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.utils.Props;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class JdbcFlowTriggerLoaderImpl implements FlowTriggerLoader {

  private static final Logger logger = LoggerFactory.getLogger(JdbcFlowTriggerLoaderImpl.class);

  private static final String[] DEPENDENCY_EXECUTIONS_COLUMNS = {"trigger_instance_id", "dep_name",
      "starttime", "endtime", "dep_status", "cancelleation_cause", "project_id", "project_version",
      "flow_id", "flow_version", "project_json", "flow_exec_id"};

  private static final String DEPENDENCY_EXECUTION_TABLE = "execution_dependencies";

  private static final String INSERT_DEPENDENCY = String.format("INSERT INTO %s(%s) VALUES(%s);"
      + "", DEPENDENCY_EXECUTION_TABLE, StringUtils.join
      (DEPENDENCY_EXECUTIONS_COLUMNS, ","), String.join(",", Collections.nCopies
      (DEPENDENCY_EXECUTIONS_COLUMNS.length, "?")));

  private static final String UPDATE_DEPENDENCY_STATUS = String.format("UPDATE %s SET dep_status "
      + "= ? WHERE trigger_instance_id = ? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private static final String UPDATE_DEPENDENCY_STATUS_ENDTIME_AND_CANCELLEATION_CAUSE = String
      .format
          ("UPDATE %s SET dep_status = ?, endtime = ?, cancelleation_cause  = ? WHERE trigger_instance_id = "
              + "? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private static final String UPDATE_DEPENDENCY_STATUS_ENDTIME = String.format("UPDATE %s SET "
          + "dep_status = ?, endtime = ? WHERE trigger_instance_id = ? AND dep_name = ?;",
      DEPENDENCY_EXECUTION_TABLE);

  //todo chengren311: avoid scanning the whole table
  private static final String SELECT_ALL_EXECUTIONS =
      String.format("SELECT %s FROM %s ",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_EXECUTIONS_BY_INSTANCE_ID =
      String.format("SELECT %s FROM %s WHERE trigger_instance_id = ?",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_ALL_PENDING_EXECUTIONS =
      String
          .format(
              "SELECT %s FROM %s WHERE trigger_instance_id in (SELECT trigger_instance_id FROM %s "
                  + "WHERE "
                  + "dep_status = %s or dep_status = %s or flow_exec_id = %s)",
              StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
              DEPENDENCY_EXECUTION_TABLE,
              DEPENDENCY_EXECUTION_TABLE,
              Status.RUNNING.ordinal(), Status.CANCELLING.ordinal(), Constants.UNASSIGNED_EXEC_ID);

  private static final String SELECT_RECENTLY_FINISHED =
      "SELECT execution_dependencies.trigger_instance_id,dep_name,starttime,endtime,dep_status,"
          + "cancelleation_cause,project_id,project_version,flow_id,flow_version,"
          + "project_json, flow_exec_id \n"
          + "FROM execution_dependencies JOIN (\n"
          + "SELECT trigger_instance_id FROM execution_dependencies WHERE trigger_instance_id not in (\n"
          + "SELECT distinct(trigger_instance_id)  FROM execution_dependencies WHERE dep_status =  0 or dep_status = 4)\n"
          + "GROUP BY trigger_instance_id\n"
          + "ORDER BY  min(starttime) desc limit %s) temp on execution_dependencies"
          + ".trigger_instance_id in (temp.trigger_instance_id);";

  private static final String UPDATE_DEPENDENCY_FLOW_EXEC_ID = String.format("UPDATE %s SET "
      + "flow_exec_id "
      + "= ? WHERE trigger_instance_id = ? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private final DatabaseOperator dbOperator;
  private final ProjectLoader projectLoader;


  @Inject
  public JdbcFlowTriggerLoaderImpl(final DatabaseOperator databaseOperator,
      final ProjectLoader projectLoader) {
    this.dbOperator = databaseOperator;
    this.projectLoader = projectLoader;
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
    final ProjectLoader projectLoader = new JdbcProjectImpl(props, databaseOperator);
    final JdbcFlowTriggerLoaderImpl depLoader = new JdbcFlowTriggerLoaderImpl(databaseOperator,
        projectLoader);

    //final Collection<TriggerInstance> unfinished = depLoader.getIncompleteTriggerInstances();
    final List<FlowTrigger> flowTriggers = new ArrayList<>();

    final DependencyInstance depInst1 = new DependencyInstance("1", null, null, null, Status
        .CANCELLED, CancellationCause.NONE);
    final DependencyInstance depInst2 = new DependencyInstance("2", null, null, null, Status
        .CANCELLED, CancellationCause.NONE);
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    dependencyInstanceList.add(depInst1);
    dependencyInstanceList.add(depInst2);

    final TriggerInstance triggerInstance = new TriggerInstance("1", null, new FlowConfigID(1,
        1, "1", 1), null, dependencyInstanceList, -1, null);

    depLoader.uploadTriggerInstance(triggerInstance);
    System.out.println();
    //flowTriggers.add(FlowTriggerUtil.createRealFlowTrigger());

//    final Collection<TriggerInstance> triggerInstances = depLoader
//        .loadAllDependencyInstances(flowTriggers, 200);
//    System.out.println(triggerInstances);
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
    depInst.setStatus(Status.SUCCEEDED);
    depInst.setEndTime(new Date());
    triggerInst.setFlowExecId(123123);
    depLoader.updateAssociatedFlowExecId(triggerInst);*/
  }

//  @Override
//  public void uploadFlowTrigger(final FlowTrigger flowTrigger) {
//    final Gson gson = new Gson();
//    final String jsonStr = gson.toJson(flowTrigger);
//    final byte[] jsonBytes = jsonStr.getBytes(Charsets.UTF_8);
//
//    this.executeUpdate(INSERT_FLOW_TRIGGER, flowTrigger.getProjectId(), flowTrigger
//        .getProjectVersion(), flowTrigger.getFlowId(), jsonBytes);
//  }


  @Override
  public Collection<TriggerInstance> getIncompleteTriggerInstances() {
    Collection<TriggerInstance> unfinished = Collections.EMPTY_LIST;
    try {
      unfinished = this.dbOperator
          .query(SELECT_ALL_PENDING_EXECUTIONS, new TriggerInstanceHandler());

      // backfilling flow trigger for unfinished trigger instances

      // dedup flow config id with a set to avoid downloading/parsing same flow file multiple times
      final Set<FlowConfigID> flowConfigIDSet = unfinished.stream()
          .map(TriggerInstance::getFlowConfigID).collect(Collectors.toSet());

      final Map<FlowConfigID, FlowTrigger> flowTriggers = new HashMap<>();
      for (final FlowConfigID flowConfigID : flowConfigIDSet) {
        final File tempDir = Files.createTempDir();
        try {
          final File flowFile = this.projectLoader
              .getUploadedFlowFile(flowConfigID.getProjectId(), flowConfigID.getProjectVersion(),
                  flowConfigID.getFlowId() + ".flow", flowConfigID.getFlowVersion(), tempDir);

          if (flowFile != null) {
            final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
            if (flowTrigger != null) {
              flowTriggers.put(flowConfigID, flowTrigger);
            }
          } else {
            logger.error("Unable to find flow file for " + flowConfigID);
          }
        } catch (final IOException ex) {
          logger.error("error in getting flow file", ex);
        } finally {
          FlowLoaderUtils.cleanUpDir(tempDir);
        }
      }

      for (final TriggerInstance triggerInst : unfinished) {
        triggerInst.setFlowTrigger(flowTriggers.get(triggerInst.getFlowConfigID()));
      }

    } catch (final SQLException ex) {
      handleSQLException(ex);
    }

    return unfinished;
  }

  private void handleSQLException(final SQLException ex)
      throws DependencyException {
    final String error = "exception when accessing db!";
    logger.error(error, ex);
    throw new DependencyException(error, ex);
  }

  //generate where clause as such:
  //( project_id = 4 AND project_version = 3 AND flow_id = 5 AND flow_version = 6 )
  //OR ( project_id = 1 AND project_version = 2 AND flow_id = 3 AND flow_version = 4 )
//  private String generateWhereClause(final Collection<TriggerInstance> triggerInstances) {
//    final Set<String> criteriaSet = new HashSet<>();
//    for (final TriggerInstance triggerInstance : triggerInstances) {
//      final String criteria = String.format("(%s = %s AND %s = %s AND %s = %s AND %s = %s)",
//          "project_id", triggerInstance.getFlowConfigID().getProjectId(), "project_version",
//          triggerInstance.getFlowConfigID().getProjectVersion(), "flow_id", triggerInstance
//              .getFlowConfigID().getFlowId(), "flow_version",
//          triggerInstance.getFlowConfigID().getFlowVersion());
//      criteriaSet.add(criteria);
//    }
//    return StringUtils.join(criteriaSet, " OR ");
//  }

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

  //todo chengren311: change public back to private
  private void executeUpdate(final String query, final Object... params) {
    try {
      this.dbOperator.update(query, params);
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
  }

  private void executeTransaction(final SQLTransaction<Integer> tran) {
    try {
      this.dbOperator.transaction(tran);
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
  }

  @Override
  public void uploadTriggerInstance(final TriggerInstance triggerInst) {
    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        transOperator
            .update(INSERT_DEPENDENCY, triggerInst.getId(), depInst.getDepName(),
                depInst.getStartTime(), depInst.getEndTime(), depInst.getStatus().ordinal(),
                depInst.getCancellationCause().ordinal(),
                triggerInst.getFlowConfigID().getProjectId(),
                triggerInst.getFlowConfigID().getProjectVersion(),
                triggerInst.getFlowConfigID().getFlowId(),
                triggerInst.getFlowConfigID().getFlowVersion(),
                FlowUtils.toJson(triggerInst.getProject()),
                triggerInst.getFlowExecId());
      }
      return null;
    };

    executeTransaction(insertTrigger);
  }

  @Override
  public void updateDependency(final DependencyInstance depInst) {
    executeUpdate(UPDATE_DEPENDENCY_STATUS_ENDTIME_AND_CANCELLEATION_CAUSE, depInst.getStatus()
            .ordinal(),
        depInst.getEndTime(), depInst.getCancellationCause().ordinal(),
        depInst.getTriggerInstance().getId(),
        depInst.getDepName());
  }

  @Override
  /**
   * Retrieve recently finished trigger instances, but flow trigger properties are not populated
   * into the returned trigger instances for efficiency. Flow trigger properties will be
   * retrieved only on request time.
   */
  public Collection<TriggerInstance> getRecentlyFinished(final int limit) {
    final String query = String.format(SELECT_RECENTLY_FINISHED, limit);
    try {
      return this.dbOperator.query(query, new TriggerInstanceHandler());
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    return Collections.emptyList();
  }

  @Override
  /**
   * Retrieve a trigger instance given an instance id. Flow trigger properties will also be
   * populated into the returned trigger instance.
   */
  public TriggerInstance getTriggerInstanceById(final String triggerInstanceId) {
    final String query = String.format(SELECT_EXECUTIONS_BY_INSTANCE_ID, triggerInstanceId);
    TriggerInstance triggerInstance = null;
    try {
      final Collection<TriggerInstance> res = this.dbOperator
          .query(query, new TriggerInstanceHandler());
      triggerInstance = !res.isEmpty() ? res.iterator().next() : null;
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    if (triggerInstance != null) {
      final int projectId = triggerInstance.getProject().getId();
      final int projectVersion = triggerInstance.getProject().getVersion();
      final String flowFileName = triggerInstance.getFlowName() + ".flow";
      final int flowVersion = triggerInstance.getFlowConfigID().getFlowVersion();
      final File tempDir = Files.createTempDir();
      try {
        final File flowFile = this.projectLoader
            .getUploadedFlowFile(projectId, projectVersion, flowFileName, flowVersion, tempDir);

        if (flowFile != null) {
          final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
          if (flowTrigger != null) {
            triggerInstance.setFlowTrigger(flowTrigger);
          }
        } else {
          logger.error("Unable to find flow file for " + triggerInstanceId);
        }
      } catch (final IOException ex) {
        logger.error("error in getting flow file", ex);
      } finally {
        FlowLoaderUtils.cleanUpDir(tempDir);
      }
    }
    return triggerInstance;
  }

  /*
  @Override
  public Collection<TriggerInstance> loadUnfinishedDependencyInstances(
      final List<FlowTrigger> flowTriggers) {
    try {
      return this.dbOperator.query(SELECT_ALL_PENDING_EXECUTIONS, new TriggerInstanceHandler
          (flowTriggers));
    } catch (final SQLException ex) {
      throw new DependencyException("Query :" + SELECT_ALL_PENDING_EXECUTIONS + " failed.", ex);
    }
  }*/


  private static class TriggerInstanceHandler implements
      ResultSetHandler<Collection<TriggerInstance>> {

    //private final Map<String, FlowTrigger> flowTriggers;

    public TriggerInstanceHandler() {
//      this.flowTriggers = new HashMap<>();
//      for (final FlowTrigger flowTrigger : flowTriggers) {
//        this.flowTriggers.put(generateFlowTriggerKey(flowTrigger.getProjectId(), flowTrigger
//            .getProjectVersion(), flowTrigger.getFlowId()), flowTrigger);
//      }
    }

    private String generateFlowTriggerKey(final int projId, final int projVersion,
        final String flowId) {
      return projId + "," + projVersion + "," + flowId;
    }

    @Override
    public Collection<TriggerInstance> handle(final ResultSet rs) throws SQLException {
      final Map<TriggerInstKey, List<DependencyInstance>> triggerInstMap = new HashMap<>();
      //todo chengren311: get submitUser from another table with projId, projectVersion
      final String submitUser = "test";

      while (rs.next()) {
        final String triggerInstId = rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[0]);
        final String depName = rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[1]);
        final Date startTime = rs.getTimestamp(DEPENDENCY_EXECUTIONS_COLUMNS[2]);
        final Date endTime = rs.getTimestamp(DEPENDENCY_EXECUTIONS_COLUMNS[3]);
        final Status status = Status.values()[rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[4])];
        final CancellationCause cause = CancellationCause.values()[rs.getInt
            (DEPENDENCY_EXECUTIONS_COLUMNS[5])];
        final int projId = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[6]);
        final int projVersion = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[7]);
        final String flowId = rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[8]);
        final int flowVersion = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[9]);
        final Project project = FlowUtils
            .toProject(rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[10]));
        final int flowExecId = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[11]);

        final TriggerInstKey key = new TriggerInstKey(triggerInstId, submitUser, projId,
            projVersion, flowId, flowVersion, flowExecId, project);
        List<DependencyInstance> dependencyInstanceList = triggerInstMap.get(key);
        final DependencyInstance depInst = new DependencyInstance(depName, startTime, endTime,
            null, status, cause);
        if (dependencyInstanceList == null) {
          dependencyInstanceList = new ArrayList<>();
          triggerInstMap.put(key, dependencyInstanceList);
        }

        dependencyInstanceList.add(depInst);
      }

      final List<TriggerInstance> res = new ArrayList<>();
      for (final Map.Entry<TriggerInstKey, List<DependencyInstance>> entry : triggerInstMap
          .entrySet()) {
        res.add(new TriggerInstance(entry.getKey().triggerInstId, null, entry.getKey()
            .flowConfigID, entry.getKey().submitUser, entry.getValue(), entry.getKey()
            .flowExecId, entry.getKey().project));
      }

      //sort on startTrigger time
      Collections.sort(res, (o1, o2) -> {
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

      return res;
    }

    private static class TriggerInstKey {

      String triggerInstId;
      FlowConfigID flowConfigID;
      String submitUser;
      int flowExecId;
      Project project;

      public TriggerInstKey(final String triggerInstId, final String submitUser, final int projId,
          final int projVersion, final String flowId, final int flowVerion, final int flowExecId,
          final Project project) {
        this.triggerInstId = triggerInstId;
        this.flowConfigID = new FlowConfigID(projId, projVersion, flowId, flowVerion);
        this.submitUser = submitUser;
        this.flowExecId = flowExecId;
        this.project = project;
      }

      @Override
      public boolean equals(final Object o) {
        if (this == o) {
          return true;
        }

        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        final TriggerInstKey that = (TriggerInstKey) o;

        return new EqualsBuilder()
            .append(this.triggerInstId, that.triggerInstId)
            .append(this.flowConfigID, that.flowConfigID)
            .isEquals();
      }

      @Override
      public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(this.triggerInstId)
            .append(this.flowConfigID)
            .toHashCode();
      }
    }
  }
}
