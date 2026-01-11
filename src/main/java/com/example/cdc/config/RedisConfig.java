package com.example.cdc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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

    @Value("${redis.pool.max-total:8}")
    private int maxTotal;

    @Value("${redis.pool.max-idle:8}")
    private int maxIdle;

    @Value("${redis.pool.min-idle:0}")
    private int minIdle;

    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);

        if (redisPassword != null && !redisPassword.trim().isEmpty()) {
            return new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPassword, redisDatabase);
        } else {
            return new JedisPool(poolConfig, redisHost, redisPort, 2000);
        }
    }
}
