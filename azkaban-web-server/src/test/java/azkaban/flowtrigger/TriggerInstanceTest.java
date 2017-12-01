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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import azkaban.project.FlowConfigID;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TriggerInstanceTest {

  private DependencyInstance createTestDependencyInstance(final Status status, final KillingCause
      killingCause) {
    final DependencyInstance depInst = new DependencyInstance(null, null, null, null, null,
        null);
    depInst.setStatus(status);
    depInst.setKillingCause(killingCause);
    return depInst;
  }

  @Test
  public void testTriggerInstanceKillingCause() throws Exception {

    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getKillingCause()).isEqualTo(KillingCause.MANUAL);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.FAILURE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getKillingCause()).isEqualTo(KillingCause.FAILURE);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.TIMEOUT));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.TIMEOUT));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getKillingCause()).isEqualTo(KillingCause.TIMEOUT);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.TIMEOUT));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getKillingCause()).isEqualTo(KillingCause.TIMEOUT);
    dependencyInstanceList.clear();

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getKillingCause()).isEqualTo(KillingCause.NONE);
  }

  @Test
  public void testTriggerInstanceStatus() throws Exception {

    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.TIMEOUT);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.TIMEOUT);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.TIMEOUT);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLING, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.KILLING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.RUNNING, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.KILLING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.FAILED);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLING, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.RUNNING, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.KILLING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.FAILED);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.RUNNING, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLING, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.KILLING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.RUNNING, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLING, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.KILLING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.TIMEOUT);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.RUNNING, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.KILLING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.FAILED);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.RUNNING, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.RUNNING);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));
    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.SUCCEEDED);
    dependencyInstanceList.clear();

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.SUCCEEDED);
    dependencyInstanceList.clear();

    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.MANUAL));
    dependencyInstanceList.add(createTestDependencyInstance(Status.FAILED, KillingCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.FAILED);
    dependencyInstanceList.clear();
  }

}
