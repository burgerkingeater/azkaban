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

import azkaban.Constants;
import azkaban.executor.ExecutorManager;
import azkaban.flowtrigger.database.FlowTriggerLoader;
import azkaban.project.FlowConfigID;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import azkaban.project.ProjectManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
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
  private static final int RECENTLY_FINISHED_TRIGGER_LIMIT = 20;
  //private static final Duration KILLING_GRACE_PERIOD_AFTER_RESTART = Duration.ofMinutes(5);
  private static final Duration KILLING_GRACE_PERIOD_AFTER_RESTART = Duration.ofMinutes(1);
  private static final String START_TIME = "starttime";
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
      triggerProcessor, final DependencyProcessor
      dependencyProcessor, final ProjectManager projectManager,
      final ExecutorManager executorManager,
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
//    final FlowTriggerService service = new FlowTriggerService(new FlowTriggerPluginManager
//        (), new TriggerProcessor(null, null, null), new DependencyProcessor(null),
//        null, null, null);
//
//    final DependencyInstanceConfig depInstConfig = new DependencyInstanceConfigImpl(
//        new HashMap<>());
//
//    final CronSchedule validSchedule = new CronSchedule("* * * * * *");
//    final List<FlowTriggerDependency> validDependencyList = new ArrayList<>();
//    validDependencyList.add(new FlowTriggerDependency("a", "a", new HashMap<>()));
//    final Duration validDuration = Duration.ofSeconds(2);
//
//    final FlowTrigger flowTrigger = FlowTriggerUtil.createFlowTrigger();
//    final String submitUser = "test";
//
//    //service.start("test", depInstConfig, Duration.ofSeconds(2));
//    service.start(flowTrigger, submitUser);
//    //logger.info("sleeping...");
//    Thread.sleep(10 * 1000);
//    //System.out.println(service.runningTriggerContainer);
  }

  private DependencyInstanceContext createDepContext(final FlowTriggerDependency dep, final long
      starttimeInMills) {
    final DependencyCheck dependencyCheck = this.triggerPluginManager
        .getDependencyCheck(dep.getType());
    final DependencyInstanceCallback callback = new DependencyInstanceCallbackImpl(this);
    final DependencyInstanceConfigImpl config = new DependencyInstanceConfigImpl(dep.getProps());
    final DependencyInstanceRuntimeProps runtimeProps = new DependencyInstanceRuntimePropsImpl
        (ImmutableMap.of(START_TIME, String.valueOf(starttimeInMills)));
    //todo chengren311: how to handle construction of context failure or run throws exception?
    // what shoult it return??
    return dependencyCheck.run(config, runtimeProps, callback);
  }

  private TriggerInstance createTriggerInstance(final FlowTrigger flowTrigger,
      final FlowConfigID flowConfigID, final String submitUser) {
    final String execId = generateId();
    logger.info(String.format("Starting the flow trigger %s[execId %s] by %s", flowTrigger, execId,
        submitUser));
    final long startTime = System.currentTimeMillis();

    final List<DependencyInstance> depInstList = new ArrayList<>();
    for (final FlowTriggerDependency dep : flowTrigger.getDependencies()) {
      final String depName = dep.getName();
      final Date startDate = new Date(startTime);
      final Date endDate = null;
      final DependencyInstanceContext context = createDepContext(dep, startTime);
      final Status status = Status.RUNNING;
      final DependencyInstance depInst = new DependencyInstance(depName, startDate, endDate,
          context, status, KillingCause.NONE);
      depInstList.add(depInst);
    }

    final String triggerInstId = generateId();
    final int flowExecId = Constants.DEFAULT_EXEC_ID;
    final TriggerInstance triggerInstance = new TriggerInstance(triggerInstId, flowTrigger,
        flowConfigID, submitUser, depInstList, flowExecId);

    return triggerInstance;
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }

  private void scheduleKill(final String execId, final Duration duration, final KillingCause
      cause) {
    this.timeoutService.schedule(() -> {
      kill(execId, cause);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  public Collection<TriggerInstance> getRunningTriggers() {
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

  public Collection<TriggerInstance> getRecentlyFinished() {
    return this.dependencyLoader.getRecentlyFinished(RECENTLY_FINISHED_TRIGGER_LIMIT);
  }

  private void recover(final TriggerInstance triggerInstance) {
    logger.info(String.format("recovering unfinished trigger instance %s", triggerInstance.getId
        ()));
    final FlowTrigger flowTrigger = triggerInstance.getFlowTrigger();
    KillingCause cause = KillingCause.NONE;
    for (final DependencyInstance depInst : triggerInstance.getDepInstances()) {
      if (depInst.getStatus() == Status.RUNNING || depInst.getStatus() == Status.KILLING) {
        final FlowTriggerDependency dependency = flowTrigger
            .getDependencyByName(depInst.getDepName());
        final DependencyInstanceContext context = createDepContext(dependency,
            depInst.getStartTime().getTime());
        //todo chengren311: if context is null, then set status to failed
        depInst.setDependencyInstanceContext(context);
        if (depInst.getStatus() == Status.KILLING) {
          cause = depInst.getKillingCause();
        }
      }
    }

    if (triggerInstance.getStatus() == Status.KILLING) {
      // kill immediately
      addToRunningListAndScheduleKill(triggerInstance, Duration.ofSeconds(1), cause);
    } else if (triggerInstance.getStatus() == Status.RUNNING) {
      final long remainingTime = remainingTimeBeforeTimeout(triggerInstance);
      addToRunningListAndScheduleKill(triggerInstance, Duration.ofMillis(remainingTime).plus
          (KILLING_GRACE_PERIOD_AFTER_RESTART), KillingCause.TIMEOUT);
    }
  }

  public void recoverUnfinishedTriggerInstances() {
    final Collection<TriggerInstance> unfinishedTriggerInstances = this.dependencyLoader
        .getUnfinishedTriggerInstances();
    //todo chengren311: what if flow trigger is not found?
    for (final TriggerInstance triggerInstance : unfinishedTriggerInstances) {
      if (triggerInstance.getFlowTrigger() != null) {
        recover(triggerInstance);
      } else {
        logger.info(String.format("cannot recover the trigger instance %s, flow trigger is null ",
            triggerInstance.getId()));
      }
    }
  }

  private void addToRunningListAndScheduleKill(final TriggerInstance triggerInst, final
  Duration durationBeforeKill, final KillingCause cause) {
    this.executorService.submit(() -> {
      // if trigger has no dependencies, then skip following steps and execute the flow immediately
      if (!triggerInst.getDepInstances().isEmpty()) {
        this.runningTriggers.add(triggerInst);
        scheduleKill(triggerInst.getId(), durationBeforeKill, cause);
      }
    });
  }

  private long remainingTimeBeforeTimeout(final TriggerInstance triggerInst) {
    final long now = System.currentTimeMillis();
    return Math.max(0, triggerInst.getFlowTrigger().getMaxWaitDuration().toMillis() - (now -
        triggerInst.getStartTime().getTime()));
  }

  public void start(final FlowTrigger flowTrigger, final FlowConfigID flowConfigID, final String
      submitUser) {
    final TriggerInstance triggerInst = createTriggerInstance(flowTrigger, flowConfigID,
        submitUser);
    //todo chengren311: it's possible web server restarts before the db update, then
    // new instance will not be recoverable from db.
    this.triggerProcessor.processStatusUpdate(triggerInst);
    addToRunningListAndScheduleKill(triggerInst, triggerInst.getFlowTrigger()
        .getMaxWaitDuration(), KillingCause.TIMEOUT);
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

  private void processStatusUpdate(final DependencyInstance depInst, final Status status) {
    depInst.updateStatus(status);
    if (Status.isDone(depInst.getStatus())) {
      depInst.updateEndTime(new Date());
    }
    this.dependencyProcessor.processStatusUpdate(depInst);
  }

  private TriggerInstance findTriggerInstById(final String triggerInstId) {
    return this.runningTriggers.stream()
        .filter(triggerInst -> triggerInst.getId().equals(triggerInstId)).findFirst().orElse(null);
  }

  private void removeTriggerInstById(final String triggerInstId) {
    for (final Iterator<TriggerInstance> it = this.runningTriggers.iterator(); it.hasNext(); ) {
      if (triggerInstId.equals(it.next().getId())) {
        it.remove();
      }
    }
  }

  public void kill(final String triggerInstanceId, final KillingCause cause) {
    this.executorService.submit(
        () -> {
          logger.warn(String.format("killing trigger instance with id %s", triggerInstanceId));
          final TriggerInstance triggerInst = FlowTriggerService.this.findTriggerInstById
              (triggerInstanceId);
          if (triggerInst != null && triggerInst.getStatus() != Status.KILLING) {
            for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
              // kill only running dependencies, no need to kill a killed/successful dependency
              // instance
              if (depInst.getStatus() == Status.RUNNING) {
                depInst.setKillingCause(cause);
                processStatusUpdate(depInst, Status.KILLING);
                depInst.getContext().kill();
              }
            }
          } else {
            final String status = triggerInst == null ? "non-running" : "killing";
            logger.warn(String
                .format("unable to kill a trigger instance in %s state with id %s", status,
                    triggerInstanceId));
          }
        }
    );
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

        processStatusUpdate(depInst, Status.SUCCEEDED);
        if (depInst.getTriggerInstance().getStatus() == Status.SUCCEEDED) {
          this.triggerProcessor.processStatusUpdate(depInst.getTriggerInstance());
          this.runningTriggers.remove(depInst.getTriggerInstance());
        }
      } else {
        logger.warn(String.format("unable to find trigger instance with context %s when marking "
                + "it success",
            context));
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

        //todo chengren311: failure status
        final KillingCause cause = depInst.getKillingCause();
        final Status finalStatus = cause == KillingCause.TIMEOUT ? Status.TIMEOUT : Status.KILLED;

        processStatusUpdate(depInst, finalStatus);

        if (depInst.getTriggerInstance().getStatus() == finalStatus) {
          this.triggerProcessor.processStatusUpdate(depInst.getTriggerInstance());
          removeTriggerInstById(depInst.getTriggerInstance().getId());
        }
      } else {
        logger.warn(String.format("unable to find trigger instance with context %s when marking "
            + "it killed or timeout", context));
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
