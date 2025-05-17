package org.apache.hugegraph.pd.license;

import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseParam;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

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
            throw new PDRuntimeException(ErrorType.LICENSE_VERIFY_ERROR_VALUE,
                                         "Failed to read extra params", e);
        }
        return null;
    }
}
