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

package azkaban.flowtrigger;

import azkaban.executor.ExecutorManager;
import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import azkaban.project.ProjectManager;
import azkaban.test.TestDependencyCheck;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adopted from DagService.java
 * which is part of the undergoing DAG processing engine refactoring
 */
@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
public class FlowDependencyService {

  private static final long SHUTDOWN_WAIT_TIMEOUT = 60;
  private static final Logger logger = LoggerFactory.getLogger(FlowDependencyService.class);
  private final ExecutorService executorService;
  private final List<TriggerInstance> runningTriggers;
  private final ScheduledExecutorService timeoutService;
  private final FlowTriggerPluginManager triggerPluginManager;
  private final TriggerProcessor triggerProcessor;
  private final DependencyProcessor dependencyProcessor;
  private final ProjectManager projectManager;
  private final ExecutorManager executorManager;

  public FlowDependencyService() {
    final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FlowTrigger-service").build();
    this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
    this.timeoutService = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    this.runningTriggers = new ArrayList<>();
    this.triggerPluginManager = null;
    this.triggerProcessor = null;
    this.dependencyProcessor = null;
  }

  public FlowDependencyService(final FlowTriggerPluginManager pluginManager, final TriggerProcessor
      triggerProcessor, final DependencyProcessor dependencyProcessor, final ProjectManager
      projectManager, final ExecutorManager executorManager) {
    // Give the thread a name to make debugging easier.
    final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FlowTrigger-service").build();
    this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
    this.timeoutService = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    this.runningTriggers = new ArrayList<>();
    this.triggerPluginManager = pluginManager;
    this.triggerProcessor = triggerProcessor;
    this.dependencyProcessor = dependencyProcessor;
    this.projectManager = projectManager;
    this.executorManager = executorManager;
  }

  public static void main(final String[] args) throws InterruptedException {
    final FlowDependencyService service = new FlowDependencyService(new FlowTriggerPluginManager
        (), new TriggerProcessor(), new DependencyProcessor(), null, null);
    final DependencyInstanceConfig depInstConfig = new DependencyInstanceConfigImpl(
        new HashMap<>());

    final CronSchedule validSchedule = new CronSchedule("* * * * ? *");
    final List<FlowTriggerDependency> validDependencyList = new ArrayList<>();
    validDependencyList.add(new FlowTriggerDependency("a", "a", new HashMap<>()));
    final Duration validDuration = Duration.ofSeconds(2);

    final FlowTrigger flowTrigger = new FlowTrigger(validSchedule, validDependencyList,
        validDuration, null, null);

    //service.start("test", depInstConfig, Duration.ofSeconds(2));
    service.start(flowTrigger);
    //logger.info("sleeping...");
    Thread.sleep(10 * 1000);
    //System.out.println(service.runningTriggerContainer);
  }

  private DependencyCheck getDepCheck(final DependencyInstanceConfig config) {
    return new TestDependencyCheck();
  }

  private TriggerInstance createTriggerInstance(final FlowTrigger flowTrigger) {
    final String execId = getExecId();
    final TriggerInstance triggerInstance = new TriggerInstance(execId);
    for (final FlowTriggerDependency dep : flowTrigger.getDependencies()) {
      final DependencyCheck dependencyCheck = this.triggerPluginManager
          .getDependencyCheck(dep.getType());
      final DependencyInstanceCallback callback = new DependencyInstanceCallbackImpl(this);
      final DependencyInstanceConfig config = new DependencyInstanceConfigImpl(dep.getProps());
      final DependencyInstance depInst = new DependencyInstance(
          dependencyCheck.run(config, callback), triggerInstance);
      triggerInstance.addDependencyInstance(depInst);
    }
    return triggerInstance;
  }

  private String getExecId() {
    return UUID.randomUUID().toString();
  }

  private void scheduleKill(final String execId, final Duration duration) {
    this.timeoutService.schedule(() -> {
      kill(execId, true);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void start(final FlowTrigger flowTrigger) {
    logger.info(String.format("Starting the flow trigger %s", flowTrigger));
    this.executorService.submit(() -> {
      final TriggerInstance triggerInst = createTriggerInstance(flowTrigger);
      //todo chengren311: it's possible web server restarts before the db update, then
      // new instance will not be recoverable from db. We can update DB first before updating
      // memory(like write-ahead logging), but this would be very inefficient in the context of
      // single threading.
      this.triggerProcessor.processStatusUpdate(triggerInst);
      this.runningTriggers.add(triggerInst);
      scheduleKill(triggerInst.getExecId(), flowTrigger.getMaxWaitDuration());
    });
  }

  private void updateStatus(final DependencyInstance depInst, final Status status) {
    depInst.updateStatus(status);
    if (Status.isDone(depInst.getStatus())) {
      depInst.updateEndTime(System.currentTimeMillis());
    }
    this.dependencyProcessor.processStatusUpdate(depInst);
  }

  private TriggerInstance findTriggerInstByExecId(final String execId) {
    return this.runningTriggers.stream()
        .filter(triggerInst -> triggerInst.getExecId().equals(execId)).findFirst().orElse(null);
  }

  public void kill(final String execId, final boolean killedByTimeout) {
    this.executorService.submit(() -> {
      final TriggerInstance triggerInst = this.findTriggerInstByExecId(execId);
      if (triggerInst != null) {
        for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
          if (depInst.getStatus() == Status.RUNNING) {
            depInst.setTimeoutKilling(killedByTimeout);
            updateStatus(depInst, Status.KILLING);
            depInst.getContext().kill();
          }
        }
      } else {
        logger.warn(String.format("unable to find trigger instance with exec id %s", execId));
      }
    });
  }

  private DependencyInstance findDependencyInstanceByContext(
      final DependencyInstanceContext context) {
    for (final TriggerInstance triggerInst : this.runningTriggers) {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        if (depInst.getContext() == context) {
          return depInst;
        }
      }
    }
    return null;
  }

  public void markDependencySuccess(final DependencyInstanceContext context) {
    this.executorService.submit(() -> {
      logger.warn(String.format("marking trigger instance with context %s as success", context));
      final DependencyInstance depInst = findDependencyInstanceByContext(context);
      if (depInst != null) {
        if (Status.isDone(depInst.getStatus())) {
          logger.warn(String.format("OnSuccess of %s is ignored", depInst));
          return;
        }
        updateStatus(depInst, Status.SUCCEEDED);
        if (depInst.getTriggerInstance().getStatus() == Status.SUCCEEDED) {
          this.triggerProcessor.processStatusUpdate(depInst.getTriggerInstance());
          this.runningTriggers.remove(depInst.getTriggerInstance());
        }
      } else {
        logger.warn(String.format("unable to find trigger instance with context %s", context));
      }
    });
  }

  public void markDependencyKilledOrTimeout(final DependencyInstanceContext context) {
    this.executorService.submit(() -> {
      logger.warn(String.format("killing/timing out trigger instance with context %s", context));
      final DependencyInstance depInst = findDependencyInstanceByContext(context);
      if (depInst != null) {
        if (depInst.getStatus() != Status.KILLING) {
          logger.warn(String.format("OnKilled of %s is ignored", depInst));
          return;
        }

        final Status finalStatus = depInst.isTimeoutKilling() ? Status.TIMEOUT : Status.KILLED;
        updateStatus(depInst, finalStatus);

        if (depInst.getTriggerInstance().getStatus() == finalStatus) {
          this.triggerProcessor.processStatusUpdate(depInst.getTriggerInstance());
          this.runningTriggers.remove(depInst.getTriggerInstance());
        }
      } else {
        logger.warn(String.format("unable to find trigger instance with context %s", context));
      }
    });
  }

  /**
   * Shuts down the service and wait for the tasks to finish.
   *
   * Adopted from
   * <a href="https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html">
   * the Oracle JAVA Documentation.
   * </a>
   */
  public void shutdownAndAwaitTermination() {
    this.executorService.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!this.executorService.awaitTermination(SHUTDOWN_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
        this.executorService.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!this.executorService.awaitTermination(SHUTDOWN_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
          logger.error("The DagService did not terminate.");
        }
      }
    } catch (final InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      this.executorService.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }
}
