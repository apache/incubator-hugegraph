package org.apache.hugegraph.pd.license;

import java.beans.XMLDecoder;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseManager;
import de.schlichtherle.license.LicenseNotary;
import de.schlichtherle.license.LicenseParam;
import de.schlichtherle.license.NoLicenseInstalledException;
import de.schlichtherle.xml.GenericCertificate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommonLicenseManager extends LicenseManager {
    private static final String CHARSET = "UTF-8";
    private static final int BUF_SIZE = 8 * 1024;

    public CommonLicenseManager(LicenseParam param) {
        super(param);
    }

    @Override
    protected synchronized byte[] create(LicenseContent content,
                                         LicenseNotary notary)
                                         throws Exception {
        super.initialize(content);
        this.validateCreate(content);
        GenericCertificate certificate = notary.sign(content);
        return super.getPrivacyGuard().cert2key(certificate);
    }

    @Override
    protected synchronized LicenseContent install(byte[] key,
                                                  LicenseNotary notary)
                                                  throws Exception {
        GenericCertificate certificate = super.getPrivacyGuard().key2cert(key);
        notary.verify(certificate);
        String encodedText = certificate.getEncoded();
        LicenseContent content = (LicenseContent) this.load(encodedText);
        this.validate(content);
        super.setLicenseKey(key);
        super.setCertificate(certificate);
        return content;
    }

    @Override
    protected synchronized LicenseContent verify(LicenseNotary notary)
                                                 throws Exception {
        // Load license key from preferences
        byte[] key = super.getLicenseKey();
        if (key == null) {
            String subject = super.getLicenseParam().getSubject();
            throw new NoLicenseInstalledException(subject);
        }

        GenericCertificate certificate = super.getPrivacyGuard().key2cert(key);
        notary.verify(certificate);
        String encodedText = certificate.getEncoded();
        LicenseContent content = (LicenseContent) this.load(encodedText);
        this.validate(content);
        super.setCertificate(certificate);
        return content;
    }

    @Override
    protected synchronized void validate(LicenseContent content)
                                         throws LicenseContentException {
        // Call super validate, expected to be overwritten
        super.validate(content);
    }

    protected synchronized void validateCreate(LicenseContent content)
                                               throws LicenseContentException {
        // Just call super validate is ok
        super.validate(content);
    }

    private Object load(String text) throws Exception {
        InputStream bis = null;
        XMLDecoder decoder = null;
        try {
            bis = new ByteArrayInputStream(text.getBytes(CHARSET));
            decoder = new XMLDecoder(new BufferedInputStream(bis, BUF_SIZE));
            return decoder.readObject();
        } catch (UnsupportedEncodingException e) {
            throw new LicenseContentException(String.format(
                      "Unsupported charset: %s", CHARSET));
        } finally {
            if (decoder != null) {
                decoder.close();
            }
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (Exception e) {
                log.error("load file {} error: ", text, e);
            }
        }
    }
}
