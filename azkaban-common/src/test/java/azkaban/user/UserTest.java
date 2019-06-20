/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.user;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.user.User.UserPermissions;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class UserTest {

  @Test
  public void same_user_id_different_permissions_should_not_be_equal() {
    final String userID = "user_id";
    final User user1 = new User(userID);
    user1.setPermissions(new UserPermissions(ImmutableSet.of("ADMIN")));

    //create another user with same user id
    final User user2 = new User(userID);
    user2.setPermissions(new UserPermissions(ImmutableSet.of("READ")));

    assertThat(user1.equals(user2)).isFalse();
  }

  @Test
  public void same_user_id_same_permissions_should_be_equal() {
    final String userID = "user_id";
    final UserPermissions permissions = new UserPermissions(ImmutableSet.of("ADMIN"));
    final User user1 = new User(userID);
    user1.setPermissions(permissions);

    //create another user with same user id
    final User user2 = new User(userID);
    user2.setPermissions(permissions);

    assertThat(user1.equals(user2)).isTrue();
  }
}
