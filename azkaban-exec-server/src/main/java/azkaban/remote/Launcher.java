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
import azkaban.utils.Utils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipFile;

/**
 * This class is responsible for provoking an azkaban job in an isolated execution unit.
 * e.g, a container.
 * It takes following arguments:
 * 1. local path of the project zip(or directory).
 * 2. local path of the jobtype zip(or directory).
 * 3. local path of azkaban lib directory.
 */
public class Launcher {

  private static final String PROJECT_DIR = "project";
  private static final String JOBTYPE_DIR = "jobtype";
  private static final String AZ_DIR = "azkaban_lib";
  private final Path executionDir;

  public Launcher(final Path executionDir) {
    Preconditions.checkArgument(Files.exists(executionDir));
    this.executionDir = executionDir;
  }

  public static void main(final String[] args) {
    //  Launcher
  }

  private void unzip(final Path src, final Path dest) throws IOException {
    final ZipFile zip = new ZipFile(src.toFile());
    Utils.unzip(zip, dest.toFile());
  }


  private JobTypeManager jobTypeManager;

  /**
   * Sets directories up for job run, which includes:
   * 1. Unzip project zip to project dir.
   * 2. Unzip job type zip to job type dir.
   */
  private void setup(final Path projectZip, final Path jobTypeZip) throws IOException {
    unzip(projectZip, Paths.get(this.executionDir.toString(), PROJECT_DIR));
    unzip(jobTypeZip, Paths.get(this.executionDir.toString(), JOBTYPE_DIR));
  }

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
