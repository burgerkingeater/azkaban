/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.launch;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mysql.jdbc.StringUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.Path;

class Job {

  private final ImmutableMap<String, String> params;
  private final ImmutableMap<String, String> envVars;
  private final String command;
  private final Path projectZip;
  private final Optional<Path> jobTypeZip; // might not required for some job types like shell
  private final ImmutableList<java.nio.file.Path> localResource;

  //todo: builder pattern
  public Job(final Map<String, String> params, final Map<String, String> envVars,
      final String command, final Path projectZip, final Path jobTypeZip,
      final List<java.nio.file.Path> localResource) {
    Preconditions.checkNotNull(params);
    Preconditions.checkNotNull(envVars);
    Preconditions.checkArgument(!StringUtils.isNullOrEmpty(command));
    Preconditions.checkNotNull(projectZip);
    Preconditions.checkNotNull(localResource);

    this.params = ImmutableMap.copyOf(params);
    this.envVars = ImmutableMap.copyOf(envVars);
    this.command = command;
    this.projectZip = projectZip;
    this.jobTypeZip = Optional.ofNullable(jobTypeZip);
    this.localResource = ImmutableList.copyOf(localResource);
  }
}
