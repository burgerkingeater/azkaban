/*
 * Copyright 2012 LinkedIn Corp.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class User {

  private final String userid;
  private final Set<String> roles = new HashSet<>();
  private final Set<String> groups = new HashSet<>();
  private final HashMap<String, String> properties = new HashMap<>();
  private String email = "";
  private UserPermissions userPermissions;

  public User(final String userid) {
    this.userid = userid;
  }

  public String getUserId() {
    return this.userid;
  }

  public String getEmail() {
    return this.email;
  }

  public void setEmail(final String email) {
    this.email = email;
  }

  public UserPermissions getPermissions() {
    return this.userPermissions;
  }

  public void setPermissions(final UserPermissions checker) {
    this.userPermissions = checker;
  }

  public boolean hasPermission(final String permission) {
    if (this.userPermissions == null) {
      return false;
    }
    return this.userPermissions.hasPermission(permission);
  }

  public List<String> getGroups() {
    return new ArrayList<>(this.groups);
  }

  public void clearGroup() {
    this.groups.clear();
  }

  public void addGroup(final String name) {
    this.groups.add(name);
  }

  public boolean isInGroup(final String group) {
    return this.groups.contains(group);
  }

  public List<String> getRoles() {
    return new ArrayList<>(this.roles);
  }

  public void addRole(final String role) {
    this.roles.add(role);
  }

  public boolean hasRole(final String role) {
    return this.roles.contains(role);
  }

  public String getProperty(final String name) {
    return this.properties.get(name);
  }

  @Override
  public String toString() {
    String groupStr = "[";
    for (final String group : this.groups) {
      groupStr += group + ",";
    }
    groupStr += "]";
    return this.userid + ": " + groupStr;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final User user = (User) o;

    return new EqualsBuilder()
        .append(this.userid, user.userid)
        .append(this.roles, user.roles)
        .append(this.groups, user.groups)
        .append(this.properties, user.properties)
        .append(this.email, user.email)
        .append(this.userPermissions, user.userPermissions)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(this.userid)
        .append(this.roles)
        .append(this.groups)
        .append(this.properties)
        .append(this.email)
        .append(this.userPermissions)
        .toHashCode();
  }

  public static class UserPermissions {

    Set<String> permissions;

    public UserPermissions() {
      this(new HashSet<>());
    }

    public UserPermissions(final Set<String> permissions) {
      this.permissions = permissions;
    }

    public boolean hasPermission(final String permission) {
      return this.permissions.contains(permission);
    }

    public void addPermission(final String permission) {
      this.permissions.add(permission);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final UserPermissions that = (UserPermissions) o;

      return new EqualsBuilder()
          .append(this.permissions, that.permissions)
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
          .append(this.permissions)
          .toHashCode();
    }
  }
}
