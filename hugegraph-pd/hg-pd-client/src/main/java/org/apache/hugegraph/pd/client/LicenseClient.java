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

package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;

import com.google.protobuf.ByteString;

import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import lombok.extern.slf4j.Slf4j;

@Useless("license related")
@Slf4j
public class LicenseClient extends AbstractClient {

    public LicenseClient(PDConfig config) {
        super(config);
    }

    @Override
    protected AbstractStub createStub() {
        return PDGrpc.newStub(channel);
    }

    @Override
    protected AbstractBlockingStub createBlockingStub() {
        return PDGrpc.newBlockingStub(channel);
    }

    public Pdpb.PutLicenseResponse putLicense(byte[] content) {
        Pdpb.PutLicenseRequest request = Pdpb.PutLicenseRequest.newBuilder()
                                                               .setContent(
                                                                       ByteString.copyFrom(content))
                                                               .build();
        try {
            KVPair<Boolean, Pdpb.PutLicenseResponse> pair = concurrentBlockingUnaryCall(
                    PDGrpc.getPutLicenseMethod(), request,
                    (rs) -> rs.getHeader().getError().getType().equals(Pdpb.ErrorType.OK));
            if (pair.getKey()) {
                Pdpb.PutLicenseResponse.Builder builder = Pdpb.PutLicenseResponse.newBuilder();
                builder.setHeader(okHeader);
                return builder.build();
            } else {
                return pair.getValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.debug("put license with error:{} ", e);
            Pdpb.ResponseHeader rh =
                    newErrorHeader(Pdpb.ErrorType.LICENSE_ERROR_VALUE, e.getMessage());
            return Pdpb.PutLicenseResponse.newBuilder().setHeader(rh).build();
        }
    }
}
