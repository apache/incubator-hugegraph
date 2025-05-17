package org.apache.hugegraph.pd.config;

import org.springframework.context.annotation.Configuration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhangyingjie
 * @date 2024/4/2
 **/
@Data
@Configuration
@AllArgsConstructor
@NoArgsConstructor
public class Server {
    String server;
    String token;
    String pwd;
}
