package com.example.cdc.service;

import com.example.cdc.entity.GameList;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.search.*;
import redis.clients.jedis.search.schemafields.SchemaField;
import redis.clients.jedis.search.schemafields.TextField;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RedisJsonService {

    @Autowired
    private JedisPool jedisPool;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String REDIS_KEY_PREFIX = "game:";
    private static final String INDEX_NAME = "idx:game_list";

    @PostConstruct
    public void init() {
        createSearchIndex();
    }

    /**
     * 创建 Redis Search 索引
     */
    public void createSearchIndex() {
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                // 尝试删除已存在的索引
                jedis.ftDropIndex(INDEX_NAME);
                log.info("Dropped existing index: {}", INDEX_NAME);
            } catch (Exception e) {
                log.debug("Index does not exist or cannot be dropped: {}", e.getMessage());
            }

            // 创建新索引，对 name 字段建立全文搜索索引
            Schema schema = new Schema()
                    .addTextField("$.name", 1.0)
                    .addTextField("$.description", 0.5)
                    .addTextField("$.category", 0.5);

            IndexDefinition indexDefinition = new IndexDefinition()
                    .setPrefixes(REDIS_KEY_PREFIX)
                    .setLanguage(Language.CHINESE); // 支持中文分词

            jedis.ftCreate(INDEX_NAME,
                    IndexOptions.defaultOptions().setDefinition(indexDefinition),
                    schema);

            log.info("Created Redis Search index: {}", INDEX_NAME);
        } catch (Exception e) {
            log.error("Failed to create search index", e);
            throw new RuntimeException("Failed to create search index", e);
        }
    }

    /**
     * 将游戏数据同步到 Redis JSON
     */
    public void syncGameToRedis(GameList game) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = REDIS_KEY_PREFIX + game.getId();
            String json = objectMapper.writeValueAsString(game);
            jedis.jsonSet(key, json);
            log.info("Synced game to Redis: {}", key);
        } catch (Exception e) {
            log.error("Failed to sync game to Redis: {}", game.getId(), e);
            throw new RuntimeException("Failed to sync game to Redis", e);
        }
    }

    /**
     * 批量同步游戏数据到 Redis JSON
     */
    public void syncGamesToRedis(List<GameList> games) {
        for (GameList game : games) {
            syncGameToRedis(game);
        }
        log.info("Synced {} games to Redis", games.size());
    }

    /**
     * 使用 Redis Search 进行模糊搜索
     */
    public List<Map<String, Object>> fuzzySearchByName(String name) {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            // 使用模糊搜索语法，* 表示通配符
            String queryStr = String.format("@\\$.name:*%s*", name);
            Query query = new Query(queryStr).returnFields("$");

            SearchResult searchResult = jedis.ftSearch(INDEX_NAME, query);

            for (Document doc : searchResult.getDocuments()) {
                String jsonStr = doc.getString("$");
                @SuppressWarnings("unchecked")
                Map<String, Object> gameMap = objectMapper.readValue(jsonStr, Map.class);
                results.add(gameMap);
            }

            log.info("Found {} games matching name: {}", results.size(), name);
        } catch (Exception e) {
            log.error("Failed to search games by name: {}", name, e);
            throw new RuntimeException("Failed to search games", e);
        }
        return results;
    }

    /**
     * 删除 Redis 中的游戏数据
     */
    public void deleteGameFromRedis(Long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = REDIS_KEY_PREFIX + id;
            jedis.del(key);
            log.info("Deleted game from Redis: {}", key);
        } catch (Exception e) {
            log.error("Failed to delete game from Redis: {}", id, e);
            throw new RuntimeException("Failed to delete game from Redis", e);
        }
    }

    /**
     * 获取 Redis 中的游戏数据
     */
    public GameList getGameFromRedis(Long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = REDIS_KEY_PREFIX + id;
            String json = jedis.jsonGet(key);
            if (json != null) {
                return objectMapper.readValue(json, GameList.class);
            }
            return null;
        } catch (Exception e) {
            log.error("Failed to get game from Redis: {}", id, e);
            throw new RuntimeException("Failed to get game from Redis", e);
        }
    }
}
