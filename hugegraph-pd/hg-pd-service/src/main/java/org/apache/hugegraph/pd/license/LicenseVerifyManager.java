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

package org.apache.hugegraph.pd.license;

import java.io.IOException;
import java.util.List;

import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.Pdpb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseParam;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LicenseVerifyManager extends CommonLicenseManager {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int NO_LIMIT = -1;

    public LicenseVerifyManager(LicenseParam param) {
        super(param);
    }

    @Override
    protected synchronized void validate(LicenseContent content) throws LicenseContentException {
        // Call super validate firstly to verify the common license parameters
        try {
            super.validate(content);
        } catch (LicenseContentException e) {
            // log.error("Failed to verify license", e);
            throw e;
        }
        // Verify the customized license parameters.
        getExtraParams(content);
    }

    public static ExtraParam getExtraParams(LicenseContent content) {
        List<ExtraParam> params;
        try {
            TypeReference<List<ExtraParam>> type;
            type = new TypeReference<>() {
            };
            params = MAPPER.readValue((String) content.getExtra(), type);
            if (params != null && params.size() > 0) {
                return params.get(0);
            }
        } catch (IOException e) {
            log.error("Failed to read extra params", e);
            throw new PDRuntimeException(Pdpb.ErrorType.LICENSE_VERIFY_ERROR_VALUE,
                                         "Failed to read extra params", e);
        }
        return null;
    }
}
