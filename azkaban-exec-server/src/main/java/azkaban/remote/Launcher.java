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

import azkaban.utils.Props;
import java.io.IOException;

/**
 * This class is responsible for provoking an azkaban job in an isolated execution unit.
 * e.g, a container.
 */
public class Launcher {

  private static final String EXECUTION_DIR = "execution";
  private static final String JOBTYPE_DIR = "jobtype";
  private static final String AZ_DIR = "azkaban";

  private static class ARGUMENT {

    private static final String PROJECT_PATH = "project";
  }

  //private JobTypeManager jobTypeManager;

  /*
  private static Options createOptions() {
    final Option option = new Option(ARGUMENT.PROJECT_PATH, true, "HDFS path of project zip file");
    final Options options = new Options();
    options.addOption(option);
    return options;
  }

  public static void main(final String[] args) throws ParseException {
    final Options options = createOptions();
    final CommandLineParser parser = new BasicParser();
    final CommandLine commandLine = parser.parse(options, args);
    final String projectPath = commandLine.getOptionValue(ARGUMENT.PROJECT_PATH);

    final Launcher launcher = new Launcher();
    try {
      launcher.downloadProjectArtifact(projectPath);
    } catch (final IOException ex) {
    }
  }*/

  /**
   * launch a job
   */
  private void launch(final String projectZipPath, final String jobTypeZipPath,
      final Props jobProps) throws IOException {
    //downloadProjectArtifact(projectZipPath);
    //run(jobProps);
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

  /**
   * Copies {@code src} to {@code dst}. If {@code src} does not have a scheme, it is assumed to be
   * on local filesystem.
   *
   * @param src Source {@code Path}
   * @param dst Destination {@code Path}
   * @param conf HDFS configuration
   */
  /*
  private void copySrcToDest(final Path src, final Path dst, final Configuration conf)
      throws IOException {
    final FileSystem srcFs;
    if (src.toUri().getScheme() == null) {
      srcFs = FileSystem.getLocal(conf);
    } else {
      srcFs = src.getFileSystem(conf);
    }
    final FileSystem dstFs = dst.getFileSystem(conf);
    FileUtil.copy(srcFs, src, dstFs, dst, false, true, conf);
  }

  private void downloadProjectArtifact(final String projectZipPath) throws IOException {
    final Path src = new Path(projectZipPath);
    final Path dest = new Path(EXECUTION_DIR);
    System.out.println("downloading project " + projectZipPath + ":" + " to " + EXECUTION_DIR);
    //copySrcToDest(src, dest, new Configuration());
  }*/
}
