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

package azkaban.test;

import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceContext;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestDependencyInstanceContext implements DependencyInstanceContext {

  private static final ScheduledExecutorService scheduleSerivce = Executors
      .newScheduledThreadPool(1);
  private final DependencyInstanceCallback callback;

  public TestDependencyInstanceContext(final DependencyInstanceCallback callback) {
    this.callback = callback;
    scheduleSerivce.schedule(this::onSucccess, 3, TimeUnit.SECONDS);
  }

  private void onSucccess() {
    this.callback.onSuccess(this);
  }

  @Override
  public void kill() {
    System.out.println("Killing TestDependencyInstanceContext");
    this.callback.onKilled(this);
  }
}
