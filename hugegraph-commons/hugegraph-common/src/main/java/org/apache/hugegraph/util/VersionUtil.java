/*
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

package org.apache.hugegraph.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Objects;
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
     * Compare if a version is greater than the other one (inclusive)
     * @param version   The version to be compared
     * @param other     The lower bound of the range
     * @return          true if it's greater than the other, otherwise false
     */
    public static boolean gte(String version, String other) {
        E.checkArgumentNotNull(version, "The version to match is null");
        return new Version(version).compareTo(new Version(other)) >= 0;
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
         */
        String className = clazz.getSimpleName() + ".class";
        String classPath = Objects.requireNonNull(clazz.getResource(className)).toString();
        if (!classPath.startsWith("jar:file:")) {
            // Class not from JAR
            return null;
        }
        int offset = classPath.lastIndexOf("!");
        assert offset > 0;
        // Get manifest file path
        String manifestPath = classPath.substring(0, offset + 1);
        return getImplementationVersion(manifestPath);
    }

    public static String getImplementationVersion(String manifestPath) {
        manifestPath += "/META-INF/MANIFEST.MF";

        Manifest manifest;
        try {
            manifest = new Manifest(new URL(manifestPath).openStream());
        } catch (IOException ignored) {
            return null;
        }
        return manifest.getMainAttributes()
                       .getValue(Attributes.Name.IMPLEMENTATION_VERSION);
    }

    /**
     * Get version from pom.xml
     * @return      The pom version
     */
    public static String getPomVersion() {
        String cmd = "mvn help:evaluate -Dexpression=project.version " +
                     "-q -DforceStdout";
        Process process = null;
        InputStreamReader isr = null;
        try {
            process = Runtime.getRuntime().exec(cmd);
            process.waitFor();

            isr = new InputStreamReader(process.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            return br.readLine();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (Exception ignored) {
                    // pass
                }
            }

            // Destroy child process
            if (process != null) {
                process.destroy();
            }
        }
    }

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

        private final String version;
        private final int[] parts;

        public Version(String version) {
            E.checkArgumentNotNull(version, "The version is null");
            E.checkArgument(version.matches("[0-9]+(\\.[0-9]+)*"),
                            "Invalid version format: %s", version);
            this.version = version;
            this.parts = parseVersion(version);
        }

        private static int[] parseVersion(String version) {
            String[] parts = version.split("\\.");
            int[] partsNumber = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                partsNumber[i] = Integer.parseInt(parts[i]);
            }
            return partsNumber;
        }

        public final String get() {
            return this.version;
        }

        @Override
        public int compareTo(Version that) {
            if (that == null) {
                return 1;
            }
            int[] thisParts = this.parts;
            int[] thatParts = that.parts;
            int length = Math.max(thisParts.length, thatParts.length);
            for (int i = 0; i < length; i++) {
                int thisPart = i < thisParts.length ? thisParts[i] : 0;
                int thatPart = i < thatParts.length ? thatParts[i] : 0;
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
        public int hashCode() {
            int hash = 0;
            for (int i = this.parts.length - 1; i >= 0; i--) {
                int part = this.parts[i];
                if (part == 0 && hash == 0) {
                    continue;
                }
                hash = 31 * hash + Integer.hashCode(part);
            }
            return hash;
        }

        @Override
        public String toString() {
            return this.version;
        }
    }
}
