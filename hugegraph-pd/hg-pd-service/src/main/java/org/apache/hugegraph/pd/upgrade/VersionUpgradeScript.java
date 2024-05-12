/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.upgrade;

import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;

@Useless("upgrade related")
public interface VersionUpgradeScript {

    String UNLIMITED_VERSION = "UNLIMITED_VERSION";

    /**
     * the highest version that need to run upgrade instruction
     *
     * @return high version
     */
    String getHighVersion();

    /**
     * the lowest version that need to run upgrade instruction
     *
     * @return lower version
     */
    String getLowVersion();

    /**
     * If there is no data version in the PD, whether to execute the . Generally, it corresponds
     * to 3.6.2 previous versions
     *
     * @return run when pd has no data version
     */
    boolean isRunWithoutDataVersion();

    /**
     * the scrip just run once, ignore versions
     *
     * @return run once script
     */
    boolean isRunOnce();

    /**
     * run the upgrade instruction
     */
    void runInstruction(PDConfig config);

}
