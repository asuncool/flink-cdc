# MySQL to Redis JSON Sync with Search

本项目实现了从 MySQL 数据库同步 `game_list` 表到 Redis JSON，并使用 Redis Search 进行模糊搜索的功能。

## 功能特性

1. **MySQL 到 Redis JSON 同步**：将 MySQL 中的 `game_list` 表数据同步到 Redis JSON 格式存储
2. **Redis Search 模糊搜索**：基于 Redis Search 模块对游戏名称进行模糊搜索
3. **RESTful API**：提供完整的 API 接口进行数据同步和搜索操作

## 技术栈

- Spring Boot 3.3.5
- MyBatis-Plus 3.5.6
- MySQL Connector/J 8.3.0 (com.mysql:mysql-connector-j)
- Redis Stack (包含 Redis JSON 和 Redis Search 模块)
- Jedis 5.0.2
- Redisson 3.24.3

## 前置条件

### 1. 安装 Redis Stack

Redis Stack 包含了 Redis JSON 和 Redis Search 模块。

**使用 Docker 安装：**
```bash
docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
```

**或者使用 Docker Compose：**
```yaml
version: '3.8'
services:
  redis-stack:
    image: redis/redis-stack:latest
    ports:
      - "6379:6379"
      - "8001:8001"
    environment:
      - REDIS_ARGS=--requirepass yourpassword
```

### 2. 安装 MySQL

确保 MySQL 8.0 或更高版本已安装并运行。

### 3. 初始化数据库

运行 SQL 脚本创建数据库和表：
```bash
mysql -u root -p < src/main/resources/sql/init_game_list.sql
```

## 配置说明

在 `src/main/resources/application.yml` 中配置 MySQL 和 Redis 连接信息：

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/gamedb?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: root
    driver-class-name: com.mysql.cj.jdbc.Driver

redis:
  host: localhost
  port: 6379
  password: 
  database: 0
```

## 构建和运行

### 1. 构建项目
```bash
mvn clean package -DskipTests
```

### 2. 运行项目
```bash
java -jar target/sqlserver-to-postgres-cdc-1.0.0.jar
```

或者直接使用 Maven 运行：
```bash
mvn spring-boot:run
```

## API 接口说明

### 1. 同步所有游戏到 Redis
```bash
POST http://localhost:8080/api/games/sync
```

### 2. 同步单个游戏到 Redis
```bash
POST http://localhost:8080/api/games/sync/{id}
```

### 3. 模糊搜索游戏名称
```bash
GET http://localhost:8080/api/games/search?name=王者
```

### 4. 获取所有游戏（从 MySQL）
```bash
GET http://localhost:8080/api/games/list
```

### 5. 从 Redis 获取游戏
```bash
GET http://localhost:8080/api/games/redis/{id}
```

### 6. 重新创建搜索索引
```bash
POST http://localhost:8080/api/games/reindex
```

## 使用示例

### 1. 首次使用，同步数据
```bash
# 同步所有游戏数据到 Redis
curl -X POST http://localhost:8080/api/games/sync
```

### 2. 模糊搜索
```bash
# 搜索名称包含"王者"的游戏
curl http://localhost:8080/api/games/search?name=王者

# 搜索名称包含"联盟"的游戏
curl http://localhost:8080/api/games/search?name=联盟

# 搜索名称包含"射击"的游戏
curl http://localhost:8080/api/games/search?name=射击
```

### 3. 查看所有游戏
```bash
curl http://localhost:8080/api/games/list
```

## 核心实现

### 1. GameList 实体类
定义了游戏列表的数据结构，包含 id、name、description、category、price 等字段。

### 2. RedisJsonService
核心服务类，实现了：
- `createSearchIndex()`: 创建 Redis Search 索引
- `syncGameToRedis()`: 同步单个游戏到 Redis JSON
- `syncGamesToRedis()`: 批量同步游戏到 Redis JSON
- `fuzzySearchByName()`: 使用 Redis Search 进行模糊搜索
- `getGameFromRedis()`: 从 Redis 获取游戏数据

### 3. GameListService
业务服务类，整合 MySQL 和 Redis 操作。

### 4. GameListController
REST API 控制器，提供 HTTP 接口。

## Redis Search 索引说明

项目自动创建的 Redis Search 索引结构：
- 索引名称：`idx:game_list`
- 前缀：`game:`
- 索引字段：
  - `$.name`：游戏名称（权重 1.0）
  - `$.description`：游戏描述（权重 0.5）
  - `$.category`：游戏分类（权重 0.5）
- 语言：中文（支持中文分词）

## 数据存储格式

Redis 中的数据以 JSON 格式存储：
```
Key: game:1
Value: {
  "id": 1,
  "name": "王者荣耀",
  "description": "5v5公平竞技手游",
  "category": "MOBA",
  "price": 0.0,
  "createdAt": "2024-01-01T10:00:00",
  "updatedAt": "2024-01-01T10:00:00"
}
```

## 注意事项

1. 首次启动时会自动创建 Redis Search 索引
2. Redis Stack 必须包含 JSON 和 Search 模块
3. 确保 MySQL 和 Redis 服务都在运行
4. 搜索支持中文分词和模糊匹配
5. 数据同步是手动触发的，不是自动实时同步

## 故障排查

### Redis 连接失败
- 检查 Redis 是否启动
- 确认 Redis Stack 版本包含 JSON 和 Search 模块
- 验证 application.yml 中的 Redis 配置

### MySQL 连接失败
- 检查 MySQL 服务状态
- 确认数据库 gamedb 已创建
- 验证用户名和密码

### 搜索无结果
- 确保已执行同步操作
- 检查 Redis 中是否有数据：`redis-cli KEYS game:*`
- 检查索引是否创建：`redis-cli FT.INFO idx:game_list`

## 扩展建议

1. 添加自动同步机制（使用 Spring 定时任务或 CDC）
2. 实现增量同步逻辑
3. 添加更多搜索条件（价格范围、分类筛选等）
4. 实现数据更新和删除的同步
5. 添加缓存预热机制
6. 实现分页查询
