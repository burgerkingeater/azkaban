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
  RUNNING, // a trigger is running if at least one dependency is running and the rest, if any, are
  // succeeded
  SUCCEEDED, // a trigger is succeeded if all of its dependencies succeed
  TIMEOUT, // a trigger is timeout at least one times out and rest, if any, are succeeded
  KILLED, // a trigger is killed at least one is killed and rest, if any, are succeeded
  KILLING; // otherwise, a trigger is killing state

  public static boolean isDone(final Status status) {
    return status.equals(TIMEOUT) || status.equals(KILLED) || status.equals(SUCCEEDED);
  }
}
