package com.example.cdc.controller;

import com.example.cdc.entity.GameList;
import com.example.cdc.service.GameListService;
import com.example.cdc.service.RedisJsonService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/games")
public class GameListController {

    @Autowired
    private GameListService gameListService;

    @Autowired
    private RedisJsonService redisJsonService;

    /**
     * 从 MySQL 同步所有游戏到 Redis JSON
     */
    @PostMapping("/sync")
    public Map<String, Object> syncGamesToRedis() {
        Map<String, Object> response = new HashMap<>();
        try {
            gameListService.syncAllGamesToRedis();
            response.put("success", true);
            response.put("message", "All games synced to Redis successfully");
            return response;
        } catch (Exception e) {
            log.error("Failed to sync games", e);
            response.put("success", false);
            response.put("message", "Failed to sync games: " + e.getMessage());
            return response;
        }
    }

    /**
     * 同步单个游戏到 Redis
     */
    @PostMapping("/sync/{id}")
    public Map<String, Object> syncGameToRedis(@PathVariable Long id) {
        Map<String, Object> response = new HashMap<>();
        try {
            gameListService.syncGameToRedis(id);
            response.put("success", true);
            response.put("message", "Game synced to Redis successfully");
            return response;
        } catch (Exception e) {
            log.error("Failed to sync game", e);
            response.put("success", false);
            response.put("message", "Failed to sync game: " + e.getMessage());
            return response;
        }
    }

    /**
     * 使用 Redis Search 模糊搜索游戏名称
     */
    @GetMapping("/search")
    public Map<String, Object> searchGamesByName(@RequestParam String name) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<Map<String, Object>> games = redisJsonService.fuzzySearchByName(name);
            response.put("success", true);
            response.put("count", games.size());
            response.put("data", games);
            return response;
        } catch (Exception e) {
            log.error("Failed to search games", e);
            response.put("success", false);
            response.put("message", "Failed to search games: " + e.getMessage());
            return response;
        }
    }

    /**
     * 从 MySQL 获取所有游戏
     */
    @GetMapping("/list")
    public Map<String, Object> getAllGames() {
        Map<String, Object> response = new HashMap<>();
        try {
            List<GameList> games = gameListService.getAllGames();
            response.put("success", true);
            response.put("count", games.size());
            response.put("data", games);
            return response;
        } catch (Exception e) {
            log.error("Failed to get games", e);
            response.put("success", false);
            response.put("message", "Failed to get games: " + e.getMessage());
            return response;
        }
    }

    /**
     * 从 Redis 获取游戏
     */
    @GetMapping("/redis/{id}")
    public Map<String, Object> getGameFromRedis(@PathVariable Long id) {
        Map<String, Object> response = new HashMap<>();
        try {
            GameList game = redisJsonService.getGameFromRedis(id);
            if (game != null) {
                response.put("success", true);
                response.put("data", game);
            } else {
                response.put("success", false);
                response.put("message", "Game not found in Redis");
            }
            return response;
        } catch (Exception e) {
            log.error("Failed to get game from Redis", e);
            response.put("success", false);
            response.put("message", "Failed to get game from Redis: " + e.getMessage());
            return response;
        }
    }

    /**
     * 重新创建 Redis Search 索引
     */
    @PostMapping("/reindex")
    public Map<String, Object> recreateSearchIndex() {
        Map<String, Object> response = new HashMap<>();
        try {
            redisJsonService.createSearchIndex();
            response.put("success", true);
            response.put("message", "Search index recreated successfully");
            return response;
        } catch (Exception e) {
            log.error("Failed to recreate search index", e);
            response.put("success", false);
            response.put("message", "Failed to recreate search index: " + e.getMessage());
            return response;
        }
    }
}
