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

import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseTransOperator;
import azkaban.db.SQLTransaction;
import azkaban.flowtrigger.DependencyException;
import azkaban.flowtrigger.DependencyInstance;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class JdbcDependencyLoaderImpl implements DependencyLoader {

  private static final Logger logger = LoggerFactory.getLogger(JdbcDependencyLoaderImpl.class);
  private static final String[] DEPENDENCY_EXECUTIONS_COLUMNS = {"exec_id", "dep_name", "starttime",
      "endtime", "dep_status", "project_id", "project_version", "flow_id"};
  private static final String DEPENDENCY_EXECUTION_TABLE = "dependency_executions";

  private static final String INSERT_DEPENDENCY = String.format("INSERT INTO %s(%s) VALUES(%s);"
      + "", DEPENDENCY_EXECUTION_TABLE, org.apache.commons.lang.StringUtils.join
      (DEPENDENCY_EXECUTIONS_COLUMNS, ","), String.join(",", Collections.nCopies
      (DEPENDENCY_EXECUTIONS_COLUMNS.length, "?")));

  private final DatabaseOperator dbOperator;

  @Inject
  public JdbcDependencyLoaderImpl(final DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }


  private void updateDepInst(final DatabaseTransOperator transOperator,
      final DependencyInstance depInst) {

    //transOperator.update(INSERT_DEPENDENCY, depInst.getExecId(), depInst, );
  }

  @Override
  public void createDependencies(final List<DependencyInstance> depInstList) {
    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : depInstList) {
        transOperator.update(INSERT_DEPENDENCY, depInst.getExecId(), depInst.getDepName(),
            depInst.getStartTime(), depInst.getEndTime(), depInst.getStatus(), depInst
                .getTriggerInstance().getProjectId(), null, // todo chengren311: project_version tbd
            depInst.getTriggerInstance().getFlowName());
      }
      transOperator.getConnection().commit();
      return null;
    };

    try {
      this.dbOperator.transaction(insertTrigger);
    } catch (final SQLException ex) {
      throw new DependencyException(insertTrigger + " failed.", ex);
    }
  }

  @Override
  public void updateDependency(final DependencyInstance depInst) {

  }

}
