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

package org.apache.hugegraph.api.filter;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.zip.GZIPOutputStream;

import jakarta.ws.rs.NameBinding;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;
import jakarta.ws.rs.ext.WriterInterceptor;
import jakarta.ws.rs.ext.WriterInterceptorContext;
import jakarta.inject.Singleton;

import org.slf4j.Logger;

import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.util.Log;

@Provider
@Singleton
@Compress
public class CompressInterceptor implements WriterInterceptor {

    public static final String GZIP = "gzip";

    private static final Logger LOG = Log.logger(CompressInterceptor.class);

    // Set compress output buffer size to 4KB (about 40~600 vertices)
    public static final int BUFFER_SIZE = 1024 * 4;

    @Override
    public void aroundWriteTo(WriterInterceptorContext context)
                              throws IOException, WebApplicationException {
        // If there is no annotation(like exception), we don't compress it
        if (context.getAnnotations().length > 0) {
            try {
                this.compress(context);
            } catch (Throwable e) {
                LOG.warn("Failed to compress response", e);
                /*
                 * FIXME: This will cause java.lang.IllegalStateException:
                 *  Illegal attempt to call getOutputStream() after getWriter()
                 */
                throw e;
            }
        }

        context.proceed();
    }

    private void compress(WriterInterceptorContext context)
                          throws IOException {
        // Get compress info from the @Compress annotation
        final Compress compression = getCompressAnnotation(context);
        final String encoding = compression.value();
        final int buffer = compression.buffer();

        // Update header
        MultivaluedMap<String,Object> headers = context.getHeaders();
        headers.remove("Content-Length");
        headers.add("Content-Encoding", encoding);

        // Replace output stream with new compression stream
        OutputStream output = null;
        if (encoding.equalsIgnoreCase(GZIP)) {
            output = new GZIPOutputStream(context.getOutputStream(), buffer);
        } else {
            // NOTE: Currently we just support GZIP.
            throw new WebApplicationException("Can't support: " + encoding);
        }
        context.setOutputStream(output);
    }

    private static Compress getCompressAnnotation(WriterInterceptorContext c) {
        for (Annotation annotation : c.getAnnotations()) {
            if (annotation.annotationType() == Compress.class) {
                return (Compress) annotation;
            }
        }
        throw new AssertionError("Unable find @Compress annotation");
    }

    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Compress {
        String value() default GZIP;
        int buffer() default BUFFER_SIZE;
    }
}
