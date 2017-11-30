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

import java.io.Serializable;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class FlowConfigID implements Serializable {

  private final int projectId;
  private final int projectVerison;
  private final String flowId;
  private final int flowVersion;

  public FlowConfigID(final int projectId, final int projectVerison, final String flowId,
      final int flowVersion) {
    this.projectId = projectId;
    this.projectVerison = projectVerison;
    this.flowId = flowId;
    this.flowVersion = flowVersion;
  }

  public int getProjectId() {
    return this.projectId;
  }

  public int getProjectVersion() {
    return this.projectVerison;
  }

  public String getFlowId() {
    return this.flowId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final FlowConfigID that = (FlowConfigID) o;

    return new EqualsBuilder()
        .append(this.projectId, that.projectId)
        .append(this.projectVerison, that.projectVerison)
        .append(this.flowVersion, that.flowVersion)
        .append(this.flowId, that.flowId)
        .isEquals();
  }

  @Override
  public String toString() {
    return "FlowConfigID{" +
        "projectId=" + this.projectId +
        ", projectVerison=" + this.projectVerison +
        ", flowId='" + this.flowId + '\'' +
        ", flowVersion=" + this.flowVersion +
        '}';
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(this.projectId)
        .append(this.projectVerison)
        .append(this.flowId)
        .append(this.flowVersion)
        .toHashCode();
  }

  public int getFlowVersion() {
    return this.flowVersion;
  }
}
