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

package azkaban.remote;

import azkaban.jobtype.JobTypeManager;
import azkaban.utils.Props;

/**
 * This class is responsible for executing an azkaban job in an execution environment
 * e.g, a container.
 */
public class Launcher {

  private static final String EXECUTION_DIR = "execution";
  private static final String JOBTYPE_DIR = "jobtype";
  private static final String AZ_DIR = "azkaban";

  private JobTypeManager jobTypeManager;

  /**
   * launch a job
   */
  private void launch(final String projectZipPath, final String jobTypeZipPath,
      final Props jobProps) {
    downloadProjectArtifact(projectZipPath);
    run(jobProps);
  }

  private void run(final Props jobProps) {

  }

  /**
   * Prepares for job execution. It does the following:
   * 1. Download project zip from a specified HDFS location and unzip it.
   * 2. Download corresponding job type zip from a specified HDFS location and unzip it.
   */
  private void prepare(final Props jobProps) {

  }

  private void downloadProjectArtifact(final String projectZipPath) {

  }
}
