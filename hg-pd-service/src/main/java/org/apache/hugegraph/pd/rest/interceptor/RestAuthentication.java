package org.apache.hugegraph.pd.rest.interceptor;

import java.io.IOException;
import java.util.function.Function;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.service.interceptor.Authentication;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import org.apache.hugegraph.pd.rest.API;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2023/4/28
 **/
@Slf4j
@Service
public class RestAuthentication extends Authentication implements HandlerInterceptor {

    private static final String TOKEN_KEY = "Pd-Token";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws
                                                                                                       IOException {
        try {
            String token = request.getHeader(TOKEN_KEY);
            String authority = request.getHeader("Authorization");
            if (authority == null) {
                throw new Exception("Unauthorized!");
            }
            Function<String, Boolean> tokenCall = t -> {
                if (!StringUtils.isEmpty(t)) {
                    response.addHeader(TOKEN_KEY, t);
                }
                return true;
            };
            authority = authority.replace("Basic ", "");
            return authenticate(authority, token, tokenCall);
        } catch (Exception e) {
            response.setContentType("application/json");
            response.getWriter().println(new API().toJSON(e));
            response.getWriter().flush();
            return false;
        }
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable
            ModelAndView modelAndView) {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
                                @Nullable Exception ex) {
    }
}
