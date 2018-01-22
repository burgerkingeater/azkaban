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

package azkaban.testplugin2;

import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestDependencyInstanceContext2 implements DependencyInstanceContext {

  private static final ScheduledExecutorService scheduleSerivce = Executors
      .newScheduledThreadPool(1);
  private final DependencyInstanceCallback callback;
  private final Range<Long> range;

  public TestDependencyInstanceContext2(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps,
      final DependencyInstanceCallback callback) {
    this.callback = callback;
    this.range = Range.range(Long.valueOf(runtimeProps.get("starttime")), BoundType.CLOSED,
        Long.valueOf(runtimeProps.get("starttime")) + 10, BoundType.CLOSED);
    System.out.println("range:" + new Date(this.range.lowerEndpoint()));
    System.out.println("on success in 5 secs");
    scheduleSerivce.schedule(this::onSucccess, 10, TimeUnit.SECONDS);
    //scheduleSerivce.schedule(this::onSucccess, 65, TimeUnit.SECONDS);
//    if ((new Random().nextInt()) % 2 == 0) {
//      //this dependency instance will succeed
//      scheduleSerivce.schedule(this::onSucccess, 60, TimeUnit.SECONDS);
//    } else {
//      //this dependency instance will be timed out for running too long
//      scheduleSerivce.schedule(this::onSucccess, 30, TimeUnit.SECONDS);
//    }
  }


  private void onSucccess() {
    System.out.println("on success done");
    this.callback.onSuccess(this);
  }

  @Override
  public void cancel() {
    System.out.println("Killing TestDependencyInstanceContext");
    this.callback.onCancel(this);
  }
}
