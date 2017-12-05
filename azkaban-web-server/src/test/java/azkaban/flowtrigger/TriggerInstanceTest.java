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

  private DependencyInstance createTestDependencyInstance(final Status status,
      final CancellationCause
          killingCause) {
    final DependencyInstance depInst = new DependencyInstance(null, null, null, null, null,
        null);
    depInst.setStatus(status);
    depInst.setKillingCause(killingCause);
    return depInst;
  }

  @Test
  public void testTriggerInstanceKillingCause() throws Exception {

//    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
//    TriggerInstance ti = null;
//    dependencyInstanceList.add(createTestDependencyInstance(Status.CANCELLED, CancelCause.MANUAL));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.CANCELLING, CancelCause.MANUAL));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, CancelCause.NONE));
//
//    ti = new TriggerInstance("1", null,
//        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
//    assertThat(ti.getCancellationCause()).isEqualTo(CancelCause.MANUAL);
//    dependencyInstanceList.clear();
//
//    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, CancelCause.FAILURE));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, CancelCause.MANUAL));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, CancelCause.NONE));
//
//    ti = new TriggerInstance("1", null,
//        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
//    assertThat(ti.getCancellationCause()).isEqualTo(CancelCause.FAILURE);
//    dependencyInstanceList.clear();
//
//    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, CancelCause.TIMEOUT));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, CancelCause.TIMEOUT));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, CancelCause.NONE));
//
//    ti = new TriggerInstance("1", null,
//        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
//    assertThat(ti.getCancellationCause()).isEqualTo(CancelCause.TIMEOUT);
//    dependencyInstanceList.clear();
//
//    dependencyInstanceList.add(createTestDependencyInstance(Status.KILLED, CancelCause.TIMEOUT));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.TIMEOUT, CancelCause.MANUAL));
//    dependencyInstanceList.add(createTestDependencyInstance(Status.SUCCEEDED, CancelCause.NONE));
//
//    ti = new TriggerInstance("1", null,
//        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
//    assertThat(ti.getCancellationCause()).isEqualTo(CancelCause.TIMEOUT);
//    dependencyInstanceList.clear();
//
//    ti = new TriggerInstance("1", null,
//        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
//    assertThat(ti.getCancellationCause()).isEqualTo(CancelCause.NONE);
  }

  @Test
  public void testTriggerInstanceStatus() throws Exception {

    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLED);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLED);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLING, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.RUNNING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.SUCCEEDED);
    dependencyInstanceList.clear();

    ti = new TriggerInstance("1", null,
        new FlowConfigID(1, 1, null, 1), "test", dependencyInstanceList, -1);
    assertThat(ti.getStatus()).isEqualTo(Status.SUCCEEDED);
    dependencyInstanceList.clear();

  }

}
