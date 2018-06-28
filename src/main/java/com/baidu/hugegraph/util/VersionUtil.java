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

package com.baidu.hugegraph.util;

import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public final class VersionUtil {

    /**
     * Compare if a version is inside a range [begin, end)
     * @param version   The version to be compared
     * @param begin     The lower bound of the range
     * @param end       The upper bound of the range
     * @return          true if belong to the range, otherwise false
     */
    public static boolean match(Version version, String begin, String end) {
        E.checkArgumentNotNull(version, "The version to match is null");
        return version.compareTo(new Version(begin)) >= 0 &&
               version.compareTo(new Version(end)) < 0;
    }

    /**
     * Check whether a component version is matched expected range,
     * throw an exception if it's not matched.
     * @param version   The version to be checked
     * @param begin     The lower bound of the range
     * @param end       The upper bound of the range
     * @param component The owner component of version
     */
    public static void check(Version version, String begin, String end,
                             String component) {
        E.checkState(VersionUtil.match(version, begin, end),
                     "The version %s of '%s' is not in [%s, %s)",
                     version, component, begin, end);
    }

    /**
     * Get implementation version from manifest in jar
     * @param clazz The class to be load from jar package
     * @return      The implementation version
     */
    public static String getImplementationVersion(Class<?> clazz) {
        /*
         * We don't use Package.getImplementationVersion() due to
         * a duplicate package would override the origin package info.
         * https://stackoverflow.com/questions/1272648/reading-my-own-jars-manifest
         */
        String className = clazz.getSimpleName() + ".class";
        String classPath = clazz.getResource(className).toString();
        if (!classPath.startsWith("jar:file:")) {
          // Class not from JAR
          return null;
        }
        // Get manifest
        int offset = classPath.lastIndexOf("!");
        assert offset > 0;
        String manifestPath = classPath.substring(0, offset + 1) +
                              "/META-INF/MANIFEST.MF";
        Manifest manifest = null;
        try {
            manifest = new Manifest(new URL(manifestPath).openStream());
        } catch (IOException ignored) {
            return null;
        }
        return manifest.getMainAttributes()
                       .getValue(Attributes.Name.IMPLEMENTATION_VERSION);
    }

    /**
     * class Version for compare
     * https://stackoverflow.com/questions/198431/how-do-you-compare-two-version-strings-in-java
     */
    public static class Version implements Comparable<Version> {

        public static Version of(Class<?> clazz) {
            return Version.of(clazz, null);
        }

        public static Version of(Class<?> clazz, String defaultValue) {
            String v = getImplementationVersion(clazz);
            if (v == null) {
                v = defaultValue;
            }
            return v == null ? null : new Version(v);
        }

        public static Version of(String version) {
            return new Version(version);
        }

        /************************** Version define **************************/

        private String version;

        public Version(String version) {
            E.checkArgumentNotNull(version, "The version is null");
            E.checkArgument(version.matches("[0-9]+(\\.[0-9]+)*"),
                            "Invalid version format: %s", version);
            this.version = version;
        }

        public final String get() {
            return this.version;
        }

        @Override
        public int compareTo(Version that) {
            if (that == null) {
                return 1;
            }
            String[] thisParts = this.get().split("\\.");
            String[] thatParts = that.get().split("\\.");
            int length = Math.max(thisParts.length, thatParts.length);
            for (int i = 0; i < length; i++) {
                int thisPart = i < thisParts.length ?
                               Integer.parseInt(thisParts[i]) : 0;
                int thatPart = i < thatParts.length ?
                               Integer.parseInt(thatParts[i]) : 0;
                if (thisPart < thatPart) {
                    return -1;
                }
                if (thisPart > thatPart) {
                    return 1;
                }
            }
            return 0;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that == null) {
                return false;
            }
            if (this.getClass() != that.getClass()) {
                return false;
            }
            return this.compareTo((Version) that) == 0;
        }

        @Override
        public String toString() {
            return this.version;
        }
    }
}
