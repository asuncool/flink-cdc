package com.example.cdc.service;

import com.example.cdc.entity.GameList;
import com.example.cdc.mapper.GameListMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class GameListService {

    @Autowired
    private GameListMapper gameListMapper;

    @Autowired
    private RedisJsonService redisJsonService;

    /**
     * 从 MySQL 同步所有游戏到 Redis JSON
     */
    public void syncAllGamesToRedis() {
        log.info("Starting to sync all games from MySQL to Redis...");
        List<GameList> games = gameListMapper.selectList(null);
        redisJsonService.syncGamesToRedis(games);
        log.info("Sync completed. Total games synced: {}", games.size());
    }

    /**
     * 同步单个游戏到 Redis
     */
    public void syncGameToRedis(Long id) {
        GameList game = gameListMapper.selectById(id);
        if (game != null) {
            redisJsonService.syncGameToRedis(game);
            log.info("Synced game {} to Redis", id);
        } else {
            log.warn("Game with id {} not found in MySQL", id);
        }
    }

    /**
     * 获取所有游戏
     */
    public List<GameList> getAllGames() {
        return gameListMapper.selectList(null);
    }

    /**
     * 根据 ID 获取游戏
     */
    public GameList getGameById(Long id) {
        return gameListMapper.selectById(id);
    }
}
