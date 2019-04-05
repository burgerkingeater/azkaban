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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * This class is responsible for executing an azkaban job in an isolated execution unit.
 * e.g, a container.
 * It takes following arguments:
 * 1. local path of project directory.
 * 2. local path of jobtype directory.
 * 3. local path of azkaban lib directory.
 */
public class Launcher {

  private static final String PROJECT_DIR = "project";
  private static final String JOBTYPE_DIR = "jobtype";
  private static final String AZ_DIR = "azkaban_libs";

  private final Path executionDir;
  private final Path projectDir;
  private final Path jobtypeDir;
  private final Path azLibDir;

  public Launcher(final Path executionDir, final String projectDir, final String jobtypeDir,
      final String azLibDir) {
    Preconditions.checkArgument(Files.exists(executionDir));
    this.executionDir = executionDir;
    this.projectDir = Paths.get(this.executionDir.toString(), projectDir);
    this.jobtypeDir = Paths.get(this.executionDir.toString(), jobtypeDir);
    this.azLibDir = Paths.get(this.executionDir.toString(), azLibDir);
  }

  public static void main(final String[] args) throws IOException {
    System.out.println("hihihi");
    //  Launcher
    final String projectDir = args[0];
    final String jobtypeDir = args[1];
    final String azLibDir = args[2];
    final Path currentWorkingDir = Paths.get("").toAbsolutePath();

    final Launcher launcher = new Launcher(currentWorkingDir, projectDir, jobtypeDir, azLibDir);
    launcher.setup();

  }

  /**
   * Sets directories up for job run, which includes:
   * 1. Rename project dir
   * 2. Rename job type dir
   * 3. Rename AZ lib dir
   */
  private void setup() throws IOException {
    Files.move(this.projectDir, Paths.get(this.executionDir.toString(), PROJECT_DIR));
    Files.move(this.jobtypeDir, Paths.get(this.executionDir.toString(), JOBTYPE_DIR));
    Files.move(this.azLibDir, Paths.get(this.executionDir.toString(), AZ_DIR));
  }


  /*
  private static Options createOptions() {
    final org.apache.commons.cli.Option option = new Option(ARGUMENT.PROJECT_PATH, true, "HDFS path of project zip file");
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
