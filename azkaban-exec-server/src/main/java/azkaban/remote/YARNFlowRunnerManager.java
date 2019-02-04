/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.LocalFlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.jobtype.JobTypeManager;
import azkaban.project.ProjectLoader;
import azkaban.spi.AzkabanEventReporter;
import azkaban.utils.Props;
import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;

/**
 * Execution manager for the server side execution.
 *
 * When a flow is submitted to FlowRunnerManager, it is the {@link Status#PREPARING} status. When a
 * flow is about to be executed by FlowRunner, its status is updated to {@link Status#RUNNING}
 *
 * Two main data structures are used in this class to maintain flows.
 *
 * runningFlows: this is used as a bookkeeping for submitted flows in FlowRunnerManager. It has
 * nothing to do with the executor service that is used to execute the flows. This bookkeeping is
 * used at the time of canceling or killing a flow. The flows in this data structure is removed in
 * the handleEvent method.
 *
 * submittedFlows: this is used to keep track the execution of the flows, so it has the mapping
 * between a Future<?> and an execution id. This would allow us to find out the execution ids of the
 * flows that are in the Status.PREPARING status. The entries in this map is removed once the flow
 * execution is completed.
 */
@Singleton
public class YARNFlowRunnerManager {

  private static final Logger logger = Logger.getLogger(YARNFlowRunnerManager.class);

  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobtypeManager;
  private final AzkabanEventReporter azkabanEventReporter;
  private final Props azkabanProps;
  private final File projectDirectory;

  private YARNFlowRunner runningFlow;
  private Future runningFlowFuture;
  // We want to limit the log sizes to about 20 megs
  private final String jobLogChunkSize;
  private final int jobLogNumFiles;
  // If true, jobs will validate proxy user against a list of valid proxy users.
  private final boolean validateProxyUser;

  @Inject
  public YARNFlowRunnerManager(final Props props,
      final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader,
      final File projectDirectory,
      @Nullable final AzkabanEventReporter azkabanEventReporter) {
    this.azkabanProps = props;
    this.azkabanEventReporter = azkabanEventReporter;
    this.projectDirectory = projectDirectory;
    this.executorService = Executors.newSingleThreadExecutor();
    this.executorLoader = executorLoader;
    this.projectLoader = projectLoader;
    this.jobLogChunkSize = this.azkabanProps.getString("job.log.chunk.size", "5MB");
    this.jobLogNumFiles = this.azkabanProps.getInt("job.log.backup.index", 4);
    this.validateProxyUser = this.azkabanProps.getBoolean("proxy.user.lock.down", false);
    this.jobtypeManager =
        new JobTypeManager(props.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
            JobTypeManager.DEFAULT_JOBTYPEPLUGINDIR));
  }

  public void submitFlow(final int execId) throws ExecutorManagerException {
    final YARNFlowRunner runner = createFlowRunner(execId);
    submitFlowRunner(runner);
  }

  private YARNFlowRunner createFlowRunner(final int execId) throws ExecutorManagerException {
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      throw new ExecutorManagerException("Error loading flow with exec " + execId);
    }

    // Setup flow runner
    FlowWatcher watcher = null;

    final ExecutionOptions options = flow.getExecutionOptions();
    if (options.getPipelineExecutionId() != null) {
      final Integer pipelineExecId = options.getPipelineExecutionId();

      if (this.runningFlow != null) {
        watcher = new LocalFlowWatcher(this.runningFlow);
      } else {
        // also ends up here if execute is called with pipelineExecId that's not running any more
        // (it could have just finished, for example)
        watcher = new RemoteFlowWatcher(pipelineExecId, this.executorLoader);
      }
    }

    final YARNFlowRunner runner =
        new YARNFlowRunner(flow, this.executorLoader, this.projectLoader, this.jobtypeManager,
            this.azkabanProps, this.azkabanEventReporter, this.projectDirectory);

    runner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(20);

    return runner;
  }

  private void submitFlowRunner(final YARNFlowRunner runner) {
    this.runningFlow = runner;
    System.out.println("run222");
    this.runningFlowFuture = this.executorService.submit(runner);
    try {
      this.runningFlowFuture.get();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    } catch (final ExecutionException e) {
      e.printStackTrace();
    }
  }

  public ExecutableFlow getExecutableFlow(final int execId) {
    return this.runningFlow.getExecutableFlow();
  }

}
