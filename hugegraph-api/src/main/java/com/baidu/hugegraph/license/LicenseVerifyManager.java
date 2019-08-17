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

package com.baidu.hugegraph.license;

import java.util.List;

import javax.ws.rs.core.Context;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.Log;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseParam;

public class LicenseVerifyManager extends CommonLicenseManager {

    private static final Logger LOG = Log.logger(LicenseVerifyManager.class);

    @Context
    private javax.inject.Provider<HugeConfig> configProvider;

    public LicenseVerifyManager(LicenseParam param) {
        super(param);
    }

    @Override
    protected synchronized void validate(LicenseContent content)
                                         throws LicenseContentException {
        // call super validate firstly
        super.validate(content);

        // Verify the customized license parameters.
        @SuppressWarnings("unchecked")
        List<ExtraParam> extraParams = (List<ExtraParam>) content.getExtra();
        String serverId = this.getServerId();
        LOG.debug("server id is {}", serverId);
        ExtraParam param = this.findMatchParam(serverId, extraParams);
        if (param == null) {
            throw new LicenseContentException("The current server's id " +
                                              "is not authorized");
        }

        MachineInfo machineInfo = new MachineInfo();
        if(!checkIpOrMac(param.getIp(), machineInfo.getIpAddress())) {
            throw new LicenseContentException("The current server's ip " +
                                              "is not authorized");
        }
        if (!checkIpOrMac(param.getMac(), machineInfo.getMacAddress())) {
            throw new LicenseContentException("The current server's mac " +
                                              "is not authorized");
        }
//        checkCpu();
//        checkRam();
//        checkThreads();
//        checkMemory();
    }

    private String getServerId() {
        HugeConfig config = this.configProvider.get();
        return config.get(ServerOptions.SERVER_ID);
    }

    private ExtraParam findMatchParam(String id, List<ExtraParam> extraParams) {
        for (ExtraParam param : extraParams) {
            if (param.getId().equals(id)) {
                return param;
            }
        }
        return null;
    }

    private boolean checkIpOrMac(String expect, List<String> actualList) {
        if (StringUtils.isEmpty(expect)) {
            return true;
        }
        return actualList.contains(expect);
    }
}
