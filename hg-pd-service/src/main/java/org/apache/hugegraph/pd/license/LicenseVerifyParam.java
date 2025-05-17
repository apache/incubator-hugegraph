package org.apache.hugegraph.pd.license;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LicenseVerifyParam {

    @JsonProperty("subject")
    private String subject;

    @JsonProperty("public_alias")
    private String publicAlias;

    @JsonAlias("store_ticket")
    @JsonProperty("store_password")
    private String storePassword;

    @JsonProperty("publickey_path")
    private String publicKeyPath;

    @JsonProperty("license_path")
    private String licensePath;

    public String subject() {
        return this.subject;
    }

    public String publicAlias() {
        return this.publicAlias;
    }

    public String storePassword() {
        return this.storePassword;
    }

    public String licensePath() {
        return this.licensePath;
    }

    public String publicKeyPath() {
        return this.publicKeyPath;
    }
}
