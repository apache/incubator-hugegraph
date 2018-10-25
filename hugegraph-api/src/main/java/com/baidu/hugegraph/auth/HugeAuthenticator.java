/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.auth;

import java.util.Map;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;

public interface HugeAuthenticator extends Authenticator {

    public static final String ROLE_NONE = "";
    public static final String ROLE_ADMIN = "admin";
    public static final String ROLE_USER = "user";
    public static final String ROLE_OWNER = "$owner";
    public static final String ROLE_DYNAMIC = "$dynamic";

    public void setup(HugeConfig config);

    @Override
    public User authenticate(final Map<String, String> credentials)
                             throws AuthenticationException;

    @Override
    public default boolean requireAuthentication() {
        return true;
    }

    public static class User extends AuthenticatedUser {

        protected static final String USER_ADMIN = ROLE_ADMIN;
        protected static final String USER_ANONY = ANONYMOUS_USERNAME;

        public static final User ADMIN = new User(USER_ADMIN, ROLE_ADMIN);
        public static final User ANONYMOUS = new User(USER_ANONY, ROLE_ADMIN);

        private final String role;

        public User(String username, String role) {
            super(username);
            this.role = role;
        }

        public String username() {
            return this.getName();
        }

        public String role() {
            return this.role;
        }

        @Override
        public boolean isAnonymous() {
            return this == ANONYMOUS || this == ANONYMOUS_USER;
        }

        @Override
        public int hashCode() {
            return this.username().hashCode() ^ this.role().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof User)) {
                return false;
            }

            User other = (User) obj;
            return this.username().equals(other.username()) &&
                   this.role().equals(other.role());
        }

        @Override
        public String toString() {
            return String.format("User{username=%s,role=%s}",
                                 this.username(), this.role());
        }
    }

    public static HugeAuthenticator loadAuthenticator(HugeConfig conf) {
        String authenticatorClass = conf.get(ServerOptions.AUTHENTICATOR);
        ClassLoader cl = conf.getClass().getClassLoader();

        HugeAuthenticator authenticator;
        try {
            authenticator = (HugeAuthenticator) cl.loadClass(authenticatorClass)
                                                  .newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to load authenticator: '%s'",
                                    authenticatorClass, e);
        }

        authenticator.setup(conf);

        return authenticator;
    }
}
