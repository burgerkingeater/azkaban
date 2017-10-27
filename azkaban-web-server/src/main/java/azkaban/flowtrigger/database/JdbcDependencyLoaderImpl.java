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
import azkaban.flowtrigger.DependencyInstance;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class JdbcDependencyLoaderImpl implements DependencyLoader {

  private static final String INSERT_DEPENDENCY = "INSERT INTO DEPEN()"
  private final DatabaseOperator dbOperator;

  @Inject
  public JdbcDependencyLoaderImpl(final DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  @Override
  public void createDependencies(final List<DependencyInstance> depInstList) {

  }

  @Override
  public void updateDependency(final DependencyInstance depInst) {

  }

  public static class Dependency  ResultHandler {

  }
}
