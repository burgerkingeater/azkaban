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

import azkaban.db.DBMetrics;
import azkaban.db.DatabaseOperator;
import azkaban.db.MySQLDataSource;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.executor.ActiveExecutingFlowsDao;
import azkaban.executor.AssignExecutorDao;
import azkaban.executor.ExecutionFlowDao;
import azkaban.executor.ExecutionJobDao;
import azkaban.executor.ExecutionLogsDao;
import azkaban.executor.ExecutorDao;
import azkaban.executor.ExecutorEventsDao;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.FetchActiveFlowDao;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.executor.NumExecutionsDao;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobtype.JobTypeManager;
import azkaban.metrics.MetricsManager;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectLoader;
import azkaban.utils.Props;
import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;


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
  private final Path currentWorkingDir;

  private static final Logger logger = Logger.getLogger(Launcher.class);

  public Launcher(final Path currentWorkingDir) {
    this.currentWorkingDir = currentWorkingDir;
  }

  private static void validateArgument(final String[] args) {

  }

  public static void main(final String[] args) throws IOException, ExecutorManagerException {
    //  Launcher
    final String projectDir = args[0];
    final String jobtypeDir = args[1];
    final String azLibDir = args[2];
    final String jobType = args[3];
    final String commonProperties = args[4];
    final String commonPrivateProperties = args[5];
    final String jobPropFile = args[6];
    final String tokenFilePath = args[7];
    final String mode = args[8];

    validateArgument(args);
    System.out.println(mode);
    final Path currentWorkingDir = Paths.get("").toAbsolutePath();
    final Path projectDirPath = Paths.get(currentWorkingDir.toString(), PROJECT_DIR);
    final Path jobtypePluginPath = Paths.get(currentWorkingDir.toString(), JOBTYPE_DIR);
    if (mode.equals("remote")) {

      System.out.println("moving projectDir:" + projectDir + ": to " + projectDirPath);
      System.out.println("moving jobtypeDir:" + jobtypeDir + ": to " + jobtypePluginPath);
      System.out.println("moving azlibDir:" + azLibDir + ": to " + Paths
          .get(currentWorkingDir.toString(), AZ_DIR));

      Files.move(Paths.get(projectDir), projectDirPath);


      // creating the specific job type dir
      Files.createDirectories(jobtypePluginPath);

      System.out.println("Moving commonProperties:" + commonProperties + ": to " +
          Paths.get(jobtypePluginPath.toString(), commonProperties));

      Files.move(Paths.get(commonProperties),
          Paths.get(jobtypePluginPath.toString(), commonProperties));

      System.out.println("Moving commonPrivate:" + commonPrivateProperties + ": to " +
          Paths.get(jobtypePluginPath.toString(), commonPrivateProperties));
      Files.move(Paths.get(commonPrivateProperties), Paths.get(jobtypePluginPath.toString(),
          commonPrivateProperties));

      System.out.println("Moving jobtypeDir:" + jobtypeDir + ": to " +
          Paths.get(jobtypePluginPath.toString(), jobType));
      Files.move(Paths.get(jobtypeDir), Paths.get(jobtypePluginPath.toString(), jobType));

      System.out.println("Moving azLibDir:" + azLibDir + ": to " +
          Paths.get(currentWorkingDir.toString(), AZ_DIR));
      Files.move(Paths.get(azLibDir), Paths.get(currentWorkingDir.toString(), AZ_DIR));
    }

    final Launcher launcher = new Launcher(currentWorkingDir);

    final YARNFlowRunnerManager flowRunnerManager = createFlowRunnerManager(projectDirPath,
        jobtypePluginPath);

    flowRunnerManager.submitFlow(32658);
    //final Props jobProp = loadProps(Paths.get(jobPropFile));
    //launcher.launch(jobType, jobProp, Paths.get(tokenFilePath));
  }

  private static Props loadProps(final Path propFile) throws IOException {
    return new Props(null, propFile.toFile());
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


  static DataSource createMySQLDataSource(final Props props, final DBMetrics dbMetrics) {
    return new MySQLDataSource(props, dbMetrics);
//    final int port = props.getInt("mysql.port");
//    final String host = props.getString("mysql.host");
//    final String dbName = props.getString("mysql.database");
//    final String user = props.getString("mysql.user");
//    final String password = props.getString("mysql.password");
//    final int numConnections = props.getInt("mysql.numconnections");
//    final String url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
//
//    addConnectionProperty("useUnicode", "yes");
//    addConnectionProperty("characterEncoding", "UTF-8");
//    setDriverClassName("com.mysql.jdbc.Driver");
//    setUsername(user);
//    setPassword(password);
//    setUrl(url);
//    setMaxTotal(numConnections);
//    setValidationQuery("/* ping */ select 1");
//    setTestOnBorrow(true);
  }

  private static YARNFlowRunnerManager createFlowRunnerManager(final Path projectDir,
      final Path jobtypePluginDir) {
    final Map<String, String> propMap = new HashMap<>();
    propMap.put(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR, jobtypePluginDir.toString());
    final Props props = new Props(null, propMap);

    final DataSource dataSource = createMySQLDataSource(props,
        new DBMetrics(new MetricsManager(new MetricRegistry())));

    final DatabaseOperator dbOperator = new DatabaseOperator(new QueryRunner(dataSource));
    final ExecutionFlowDao executionFlowDao = new ExecutionFlowDao(dbOperator);
    final ExecutorDao executorDao = new ExecutorDao(dbOperator);
    final ExecutionJobDao executionJobDao = new ExecutionJobDao(dbOperator);
    final ExecutionLogsDao executionLogsDao = new ExecutionLogsDao(dbOperator);
    final ExecutorEventsDao executorEventsDao = new ExecutorEventsDao(dbOperator);
    final ActiveExecutingFlowsDao activeExecutingFlowsDao = new ActiveExecutingFlowsDao(dbOperator);
    final FetchActiveFlowDao fetchActiveFlowDao = new FetchActiveFlowDao(dbOperator);
    final AssignExecutorDao assignExecutorDao = new AssignExecutorDao(dbOperator, executorDao);
    final NumExecutionsDao numExecutionsDao = new NumExecutionsDao(dbOperator);

    final ExecutorLoader executorLoader = new JdbcExecutorLoader(executionFlowDao, executorDao,
        executionJobDao, executionLogsDao, executorEventsDao, activeExecutingFlowsDao,
        fetchActiveFlowDao, assignExecutorDao, numExecutionsDao);

    final ProjectLoader projectLoader = new JdbcProjectImpl(props, dbOperator);
    final YARNFlowRunnerManager flowRunnerManager = new YARNFlowRunnerManager(props, executorLoader,
        projectLoader, projectDir.toFile(), null);
    return flowRunnerManager;
  }
  /**
   * launch a job
   */
  private void launch(final String jobType, final Props jobProps, final Path tokenFile) {
    try {
      final JobTypeManager jobTypeManager = new JobTypeManager(JOBTYPE_DIR);
      if (Files.exists(tokenFile)) {
        jobProps.put("env.HADOOP_TOKEN_FILE_LOCATION", tokenFile.toAbsolutePath().toString());
      }
      jobProps.put("working.dir", this.currentWorkingDir.toString());
      final Job job = jobTypeManager.buildJobExecutor(jobProps, logger);
      if (job instanceof AbstractProcessJob) {
//        final Props resolvedSysProps = ((AbstractProcessJob) job).getSysProps();
//        final Props resolvedJobProps = ((AbstractProcessJob) job).getJobProps();
//        final Props allProps = ((AbstractProcessJob) job).getAllProps();
//        final HadoopProxy hadoopProxy = new HadoopProxy(resolvedSysProps, resolvedJobProps, logger);
//        hadoopProxy.setupPropsForProxy(allProps, jobProps, logger);
        // read HADOOP_TOKEN_FILE from job props or env variable? System.getenv
        job.run();
      }
      System.out.println(jobTypeManager.getJobTypePluginSet().getPluginClass(jobType));
    } catch (final Exception e) {
      System.out.println(ExceptionUtils.getFullStackTrace(e));
    }
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
