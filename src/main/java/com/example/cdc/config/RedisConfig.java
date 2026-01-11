package com.example.cdc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;

@Configuration
public class RedisConfig {

    @Value("${redis.host:localhost}")
    private String redisHost;

    @Value("${redis.port:6379}")
    private int redisPort;

    @Value("${redis.password:}")
    private String redisPassword;

    @Value("${redis.database:0}")
    private int redisDatabase;

    @Bean
    public JedisPooled jedisPooled() {
        HostAndPort hostAndPort = new HostAndPort(redisHost, redisPort);
        DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                .database(redisDatabase);
        
        if (redisPassword != null && !redisPassword.trim().isEmpty()) {
            configBuilder.password(redisPassword);
        }
        
        return new JedisPooled(hostAndPort, configBuilder.build());
    }
}
