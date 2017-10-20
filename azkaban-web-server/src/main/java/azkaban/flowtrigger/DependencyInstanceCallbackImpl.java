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

import com.google.common.base.Preconditions;

public class DependencyInstanceCallbackImpl implements DependencyInstanceCallback {

  private final FlowDependencyService service;

  //todo chengren311: gucify it
  public DependencyInstanceCallbackImpl(final FlowDependencyService service) {
    Preconditions.checkNotNull(service);
    this.service = service;
  }

  @Override
  public void onSuccess(final DependencyInstanceContext depContext) {
    this.service.markDependencySuccess(depContext);
  }

  @Override
  public void onKilled(final DependencyInstanceContext depContext) {
    this.service.markDependencyKilledOrTimeout(depContext);
  }

}
