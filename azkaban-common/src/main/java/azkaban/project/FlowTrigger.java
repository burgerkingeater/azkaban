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

package azkaban.project;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FlowTrigger is the logical representation of a trigger.
 * It couldn't be changed once gets constructed.
 * It will be used to create running trigger instance.
 */
public class FlowTrigger implements Serializable {

  private final Map<String, FlowTriggerDependency> dependencies;
  private final CronSchedule schedule;
  private final Duration maxWaitDuration;
  //who to notify when trigger fails for any reason(manually killed, time out, improper trigger
  // config ...)
  //private final List<String> failureRecipients;

  //project id and flow id of the flow to be triggered
  private int projectId;
  private int projectVersion;
  private String flowId;
  private int flowVersion;


  /**
   * @throws IllegalArgumentException if any of the argument is null or there is duplicate
   * dependency name or duplicate dependency type and params
   */
  public FlowTrigger(final CronSchedule schedule, final List<FlowTriggerDependency> dependencies,
      final Duration maxWaitDuration, final int projectId, final int projectVersion,
      final String flowId, final List<String> failureRecipients) {
    Preconditions.checkNotNull(schedule);
    Preconditions.checkNotNull(dependencies);
    Preconditions.checkNotNull(maxWaitDuration);
    Preconditions.checkNotNull(this.flowId);
    Preconditions.checkArgument(maxWaitDuration.toMinutes() >= 1);
    validateDependencies(dependencies);
    this.schedule = schedule;
    final ImmutableMap.Builder builder = new Builder();
    dependencies.forEach(dep -> builder.put(dep.getName(), dep));
    this.dependencies = builder.build();
    this.maxWaitDuration = maxWaitDuration;
    //this.failureRecipients = Collections.unmodifiableList(this.failureRecipients);
  }

  /**
   * @throws IllegalArgumentException if any of the argument is null or there is duplicate
   * dependency name or duplicate dependency type and params
   */
  public FlowTrigger(final CronSchedule schedule, final List<FlowTriggerDependency> dependencies,
      final Duration maxWaitDuration) {
    Preconditions.checkNotNull(schedule);
    Preconditions.checkNotNull(dependencies);
    Preconditions.checkNotNull(maxWaitDuration);
    Preconditions.checkNotNull(this.flowId);
    Preconditions.checkArgument(maxWaitDuration.toMinutes() >= 1);
    validateDependencies(dependencies);
    this.schedule = schedule;
    final ImmutableMap.Builder builder = new Builder();
    dependencies.forEach(dep -> builder.put(dep.getName(), dep));
    this.dependencies = builder.build();
    this.maxWaitDuration = maxWaitDuration;
    //this.failureRecipients = Collections.unmodifiableList(this.failureRecipients);
  }

  public int getProjectId() {
    return this.projectId;
  }

  public void setProjectId(final int projectId) {
    this.projectId = projectId;
  }

  public int getProjectVersion() {
    return this.projectVersion;
  }

  public void setProjectVersion(final int projectVersion) {
    this.projectVersion = projectVersion;
  }

  public String getFlowId() {
    return this.flowId;
  }

  public void setFlowId(final String flowId) {
    this.flowId = flowId;
  }

  public int getFlowVersion() {
    return this.flowVersion;
  }

  public void setFlowVersion(final int flowVersion) {
    this.flowVersion = flowVersion;
  }


  /**
   * check uniqueness of dependency.name
   */
  private void validateDepNameUniqueness(final List<FlowTriggerDependency> dependencies) {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : dependencies) {
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getName()), String.format("duplicate dependency"
          + ".name %s found, dependency.name should be unique", dep.getName()));
    }
  }

  @Override
  public String toString() {
    return "FlowTrigger{" +
        "dependencies=" + this.dependencies +
        ", schedule=" + this.schedule +
        ", maxWaitDuration=" + this.maxWaitDuration +
        ", projectId=" + this.projectId +
        ", projectVersion=" + this.projectVersion +
        ", flowId='" + this.flowId + '\'' +
        '}';
  }

  /**
   * check uniqueness of dependency type and params
   */
  private void validateDepDefinitionUniqueness(final List<FlowTriggerDependency> dependencies) {
    final Set<String> seen = new HashSet<>();
    for (final FlowTriggerDependency dep : dependencies) {
      final Map<String, String> props = dep.getProps();
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getType() + ":" + props.toString()), String.format
          ("duplicate "
              + "dependency"
              + "config %s found, dependency config should be unique", dep.getName()));
    }
  }

  private void validateDependencies(final List<FlowTriggerDependency> dependencies) {
    validateDepNameUniqueness(dependencies);
    validateDepDefinitionUniqueness(dependencies);
  }

  public FlowTriggerDependency getDependencyByName(final String name) {
    return this.dependencies.get(name);
  }

  public Collection<FlowTriggerDependency> getDependencies() {
    return this.dependencies.values();
  }

  public Duration getMaxWaitDuration() {
    return this.maxWaitDuration;
  }

  public CronSchedule getSchedule() {
    return this.schedule;
  }
}
