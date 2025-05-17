package org.apache.hugegraph.pd.rest.interceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @author zhangyingjie
 * @date 2023/5/5
 **/
@Configuration
public class AuthenticationConfigurer implements WebMvcConfigurer {

    @Autowired
    RestAuthentication restAuthentication;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(restAuthentication)
                .addPathPatterns("/**")
                .excludePathPatterns("/actuator/*", "/v1/health");
    }
}
