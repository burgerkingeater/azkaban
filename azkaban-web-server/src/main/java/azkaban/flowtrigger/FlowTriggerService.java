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
import azkaban.flowtrigger.database.FlowTriggerLoader;
import azkaban.project.CronSchedule;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import azkaban.project.ProjectManager;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adopted from DagService.java
 * which is part of the undergoing DAG processing engine refactoring
 */
@SuppressWarnings("FutureReturnValueIgnored")
@Singleton
public class FlowTriggerService {

  private static final long SHUTDOWN_WAIT_TIMEOUT = 60;
  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerService.class);
  private final ExecutorService executorService;
  private final List<TriggerInstance> runningTriggers;
  //private final Map<String, FlowTrigger> flowTriggerMap;
  private final ScheduledExecutorService timeoutService;
  private final FlowTriggerPluginManager triggerPluginManager;
  private final TriggerProcessor triggerProcessor;
  private final FlowTriggerLoader dependencyLoader;
  private final DependencyProcessor dependencyProcessor;
  private final ProjectManager projectManager;
  private final ExecutorManager executorManager;

  @Inject
  public FlowTriggerService(final FlowTriggerPluginManager pluginManager, final TriggerProcessor
      triggerProcessor, final DependencyProcessor dependencyProcessor, final ProjectManager
      projectManager, final ExecutorManager executorManager,
      final FlowTriggerLoader dependencyLoader/*, final List<FlowTrigger> flowTriggerList*/) {
    // Give the thread a name to make debugging easier.
    final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FlowTrigger-service").build();
    this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
    //this.executorService = Executors.newFixedThreadPool(60);
    this.timeoutService = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    this.runningTriggers = new ArrayList<>();
    this.triggerPluginManager = pluginManager;
    this.triggerProcessor = triggerProcessor;
    this.dependencyProcessor = dependencyProcessor;
    this.projectManager = projectManager;
    this.executorManager = executorManager;
    this.dependencyLoader = dependencyLoader;
    /*
    this.flowTriggerMap = new HashMap<>();
    for (final FlowTrigger flowTrigger : flowTriggerList) {
      this.flowTriggerMap.put(generateFlowTriggerKey(flowTrigger), flowTrigger);
    }*/
  }

  public static void main(final String[] args) throws InterruptedException {
    final FlowTriggerService service = new FlowTriggerService(new FlowTriggerPluginManager
        (), new TriggerProcessor(null, null, null), new DependencyProcessor(null),
        null, null, null);

    final DependencyInstanceConfig depInstConfig = new DependencyInstanceConfigImpl(
        new HashMap<>());

    final CronSchedule validSchedule = new CronSchedule("* * * * * *");
    final List<FlowTriggerDependency> validDependencyList = new ArrayList<>();
    validDependencyList.add(new FlowTriggerDependency("a", "a", new HashMap<>()));
    final Duration validDuration = Duration.ofSeconds(2);

    final FlowTrigger flowTrigger = FlowTriggerUtil.createFlowTrigger();
    final String submitUser = "test";

    //service.start("test", depInstConfig, Duration.ofSeconds(2));
    service.start(flowTrigger, submitUser);
    //logger.info("sleeping...");
    Thread.sleep(10 * 1000);
    //System.out.println(service.runningTriggerContainer);
  }

  private String generateFlowTriggerKey(final FlowTrigger flowTrigger) {
    return flowTrigger.getProjectId() + "," + flowTrigger.getProjectVersion() + "," + flowTrigger
        .getFlowId();
  }

  private DependencyInstanceContext createDepContext(final FlowTriggerDependency dep) {
    final DependencyCheck dependencyCheck = this.triggerPluginManager
        .getDependencyCheck(dep.getType());
    final DependencyInstanceCallback callback = new DependencyInstanceCallbackImpl(this);
    final DependencyInstanceConfig config = new DependencyInstanceConfigImpl(dep.getProps());
    return dependencyCheck.run(config, callback);
  }

  private TriggerInstance createTriggerInstance(final FlowTrigger flowTrigger,
      final String submitUser) {
    final String execId = generateId();
    logger.info(String.format("Starting the flow trigger %s[execId %s] by %s", flowTrigger, execId,
        submitUser));
    final TriggerInstance triggerInstance = new TriggerInstance(execId, flowTrigger, submitUser);
    for (final FlowTriggerDependency dep : flowTrigger.getDependencies()) {
      final DependencyInstance depInst = new DependencyInstance(dep.getName(),
          createDepContext(dep), triggerInstance);
      triggerInstance.addDependencyInstance(depInst);
    }
    return triggerInstance;
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }

  private void scheduleKill(final String execId, final Duration duration) {
    this.timeoutService.schedule(() -> {
      kill(execId, true);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  public List<TriggerInstance> getRunningTriggers() {
    final Future future = this.executorService.submit(
        (Callable) () -> FlowTriggerService.this.runningTriggers);

    List<TriggerInstance> triggerInstanceList = new ArrayList<>();
    try {
      triggerInstanceList = (List<TriggerInstance>) future.get();
    } catch (final Exception ex) {
      logger.error("error in getting running triggers", ex);
    }
    return triggerInstanceList;
  }

  public void start(final FlowTrigger flowTrigger, final String submitUser) {
    this.executorService.submit(() -> {
      final TriggerInstance triggerInst = createTriggerInstance(flowTrigger, submitUser);
      //todo chengren311: it's possible web server restarts before the db update, then
      // new instance will not be recoverable from db.

      this.triggerProcessor.processStatusUpdate(triggerInst);
      // if trigger has no dependencies, then skip following steps and execute the flow immediately
      if (!triggerInst.getDepInstances().isEmpty()) {
        this.runningTriggers.add(triggerInst);
        scheduleKill(triggerInst.getId(), flowTrigger.getMaxWaitDuration());
      }
    });
  }

  private FlowTriggerDependency getFlowTriggerDepByName(final FlowTrigger flowTrigger,
      final String depName) {
    return flowTrigger.getDependencies().stream().filter(ftd -> ftd.getName().equals(depName))
        .findFirst().orElse(null);
  }

  /*
  public void resumeUnfinishedTriggerInstances() {
    final List<TriggerInstance> unfinishedTriggerInsts = this.dependencyLoader
        .loadUnfinishedTriggerInstances();

    for (final TriggerInstance triggerInst : unfinishedTriggerInsts) {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        depInst.setContext(createDepContext(triggerInst.getFlowTrigger().getDependencyByName
            (depInst.getDepName())));
      }
    }

    this.executorService.submit(() -> {
      this.runningTriggers.addAll(unfinishedTriggerInsts);
    });
  }*/

  private void updateStatus(final DependencyInstance depInst, final Status status) {
    depInst.updateStatus(status);
    if (Status.isDone(depInst.getStatus())) {
      depInst.updateEndTime(new Date());
    }
    this.dependencyProcessor.processStatusUpdate(depInst);
  }

  private TriggerInstance findTriggerInstByExecId(final String execId) {
    return this.runningTriggers.stream()
        .filter(triggerInst -> triggerInst.getId().equals(execId)).findFirst().orElse(null);
  }

  public void kill(final String execId, final boolean killedByTimeout) {
    this.executorService.submit(() -> {
      logger.warn(String.format("killing trigger instance with execId %s", execId));
      final TriggerInstance triggerInst = this.findTriggerInstByExecId(execId);
      if (triggerInst != null) {
        for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
          // kill only running dependencies, no need to kill a killed/successful dependency
          if (depInst.getStatus() == Status.RUNNING) {
            depInst.setTimeoutKilling(killedByTimeout);
            updateStatus(depInst, Status.KILLING);
            depInst.getContext().kill();
          }
        }
      } else {
        logger.warn(String.format("unable to kill a non-running trigger instance with execId %s",
            execId));
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
      final DependencyInstance depInst = findDependencyInstanceByContext(context);
      if (depInst != null) {
        logger.info(
            String.format("setting status of trigger instance with execId %s to success",
                depInst.getTriggerInstance().getId()));

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
      final DependencyInstance depInst = findDependencyInstanceByContext(context);
      if (depInst != null) {
        logger.info(
            String.format("killing/timing out status of trigger instance with execId %s",
                depInst.getTriggerInstance().getId()));

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
          logger.error("FlowDependencyService did not terminate.");
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
