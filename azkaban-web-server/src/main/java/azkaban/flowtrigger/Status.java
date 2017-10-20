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

/**
 * Represents status for trigger/dependency
 */
public enum Status {
  RUNNING, // a trigger is running if at least one dependency is running and the rest are succeeded
  SUCCEEDED, // a trigger is succeeded if all of its dependencies succeed
  TIMEOUT, // a trigger is timeout if all of its dependencies time out
  KILLED, // a trigger is killed if all of its dependencies are killed
  KILLING; //a trigger is in killing when any but not all of its dependencies is in
  // timeout/killed/killing state

  public static boolean isDone(final Status status) {
    return status.equals(TIMEOUT) || status.equals(KILLED) || status.equals(SUCCEEDED);
  }
}
