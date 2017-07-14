package com.baidu.hugegraph.api.filter;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.zip.GZIPInputStream;

import javax.inject.Singleton;
import javax.ws.rs.NameBinding;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

@Provider
@Singleton
@DecompressInterceptor.Decompress
public class DecompressInterceptor implements ReaderInterceptor {

    public static final String GZIP = "gzip";

    @Override
    public Object aroundReadFrom(ReaderInterceptorContext context)
                                 throws IOException {
        // NOTE: Currently we just support GZIP
        String encoding = context.getHeaders().getFirst("Content-Encoding");
        if (!GZIP.equalsIgnoreCase(encoding)) {
            return context.proceed();
        }
        context.setInputStream(new GZIPInputStream(context.getInputStream()));
        return context.proceed();
    }


    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Decompress {
        String value() default GZIP;
    }
}