package com.example.cdc.service;

import com.example.cdc.entity.GameList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.search.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RedisJsonService {

    @Autowired
    private JedisPooled jedisPooled;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Gson gson = new Gson();

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
        try {
            // 尝试删除已存在的索引
            try {
                jedisPooled.ftDropIndex(INDEX_NAME);
                log.info("Dropped existing index: {}", INDEX_NAME);
            } catch (Exception e) {
                log.debug("Index does not exist or cannot be dropped: {}", e.getMessage());
            }

            // 创建新索引，对 name 字段建立全文搜索索引
            IndexDefinition indexDef = new IndexDefinition(IndexDefinition.Type.JSON)
                    .setPrefixes(REDIS_KEY_PREFIX);

            Schema schema = new Schema()
                    .addTextField("$.name", 1.0)
                    .addTextField("$.description", 0.5)
                    .addTextField("$.category", 0.5);

            jedisPooled.ftCreate(INDEX_NAME, IndexOptions.defaultOptions().setDefinition(indexDef), schema);

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
        try {
            String key = REDIS_KEY_PREFIX + game.getId();
            String json = gson.toJson(game);
            jedisPooled.jsonSetWithEscape(key, json);
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
        try {
            // 使用模糊搜索语法，* 表示通配符
            String queryStr = String.format("@\\$.name:*%s*", escapeLuceneSpecialChars(name));
            Query query = new Query(queryStr);

            SearchResult searchResult = jedisPooled.ftSearch(INDEX_NAME, query);

            for (Document doc : searchResult.getDocuments()) {
                Object jsonObj = doc.get("$");
                if (jsonObj != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> gameMap = gson.fromJson(jsonObj.toString(), Map.class);
                    results.add(gameMap);
                }
            }

            log.info("Found {} games matching name: {}", results.size(), name);
        } catch (Exception e) {
            log.error("Failed to search games by name: {}", name, e);
            throw new RuntimeException("Failed to search games", e);
        }
        return results;
    }

    /**
     * Escape Lucene special characters
     */
    private String escapeLuceneSpecialChars(String input) {
        if (input == null) return "";
        // Characters that need to be escaped in Lucene queries
        String[] specialChars = {"\\", "+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "?", ":"};
        String result = input;
        for (String special : specialChars) {
            result = result.replace(special, "\\" + special);
        }
        return result;
    }

    /**
     * 删除 Redis 中的游戏数据
     */
    public void deleteGameFromRedis(Long id) {
        try {
            String key = REDIS_KEY_PREFIX + id;
            jedisPooled.del(key);
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
        try {
            String key = REDIS_KEY_PREFIX + id;
            Object json = jedisPooled.jsonGet(key);
            if (json != null) {
                return gson.fromJson(json.toString(), GameList.class);
            }
            return null;
        } catch (Exception e) {
            log.error("Failed to get game from Redis: {}", id, e);
            throw new RuntimeException("Failed to get game from Redis", e);
        }
    }
}
