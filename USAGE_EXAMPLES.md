# MySQL to Redis JSON 同步使用示例

## 快速开始指南

### 1. 环境准备

#### 安装 MySQL
```bash
# 使用 Docker 安装 MySQL
docker run -d \
  --name mysql \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=gamedb \
  mysql:8.0
```

#### 安装 Redis Stack
Redis Stack 包含 Redis JSON 和 Redis Search 模块，是本项目的核心依赖。

```bash
# 使用 Docker 安装 Redis Stack
docker run -d \
  --name redis-stack \
  -p 6379:6379 \
  -p 8001:8001 \
  redis/redis-stack:latest
```

验证 Redis Stack 安装：
```bash
# 连接到 Redis
docker exec -it redis-stack redis-cli

# 测试 JSON 模块
127.0.0.1:6379> JSON.SET test $ '{"hello":"world"}'
OK
127.0.0.1:6379> JSON.GET test
"{\"hello\":\"world\"}"

# 测试 Search 模块  
127.0.0.1:6379> FT._LIST
(empty array)
```

### 2. 数据库初始化

连接到 MySQL 并执行初始化脚本：

```bash
# 方式1：使用 mysql 命令
mysql -h localhost -u root -proot < src/main/resources/sql/init_game_list.sql

# 方式2：通过 Docker 执行
docker exec -i mysql mysql -uroot -proot < src/main/resources/sql/init_game_list.sql
```

验证数据：
```sql
mysql> USE gamedb;
mysql> SELECT * FROM game_list LIMIT 5;
+----+--------------+--------------------------------------------------+----------+-------+
| id | name         | description                                      | category | price |
+----+--------------+--------------------------------------------------+----------+-------+
|  1 | 王者荣耀     | 5v5公平竞技手游                                  | MOBA     |  0.00 |
|  2 | 和平精英     | 腾讯光子自研反恐军事竞赛体验手游                 | 射击     |  0.00 |
|  3 | 原神         | 开放世界冒险游戏                                 | RPG      |  0.00 |
|  4 | 英雄联盟     | 多人在线战术竞技游戏                             | MOBA     |  0.00 |
|  5 | 穿越火线     | 第一人称射击游戏                                 | 射击     |  0.00 |
+----+--------------+--------------------------------------------------+----------+-------+
```

### 3. 配置应用

编辑 `src/main/resources/application.yml`：

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
  password:   # 如果 Redis 设置了密码，在此填写
  database: 0

server:
  port: 8080
```

### 4. 启动应用

```bash
# 编译
mvn clean package -DskipTests

# 启动
java -jar target/sqlserver-to-postgres-cdc-1.0.0.jar

# 或者直接使用 Maven 运行
mvn spring-boot:run
```

应用启动后会自动创建 Redis Search 索引。

### 5. 使用 API

#### 5.1 同步所有游戏到 Redis

```bash
curl -X POST http://localhost:8080/api/games/sync

# 响应示例
{
  "success": true,
  "message": "All games synced to Redis successfully"
}
```

#### 5.2 验证 Redis 中的数据

```bash
# 连接到 Redis
docker exec -it redis-stack redis-cli

# 查看所有游戏 key
127.0.0.1:6379> KEYS game:*
 1) "game:1"
 2) "game:2"
 3) "game:3"
 ...

# 查看单个游戏数据
127.0.0.1:6379> JSON.GET game:1
"{\"id\":1,\"name\":\"王者荣耀\",\"description\":\"5v5公平竞技手游\",\"category\":\"MOBA\",\"price\":0.0,...}"

# 查看索引信息
127.0.0.1:6379> FT.INFO idx:game_list
```

#### 5.3 模糊搜索游戏

**搜索名称包含"王者"的游戏：**
```bash
curl "http://localhost:8080/api/games/search?name=王者"

# 响应示例
{
  "success": true,
  "count": 1,
  "data": [
    {
      "id": 1,
      "name": "王者荣耀",
      "description": "5v5公平竞技手游",
      "category": "MOBA",
      "price": 0.0,
      "createdAt": "2024-01-01T10:00:00",
      "updatedAt": "2024-01-01T10:00:00"
    }
  ]
}
```

**搜索名称包含"联盟"的游戏：**
```bash
curl "http://localhost:8080/api/games/search?name=联盟"

# 响应示例
{
  "success": true,
  "count": 1,
  "data": [
    {
      "id": 4,
      "name": "英雄联盟",
      "description": "多人在线战术竞技游戏",
      "category": "MOBA",
      "price": 0.0,
      ...
    }
  ]
}
```

**搜索名称包含"射击"的游戏（通过描述搜索）：**
```bash
curl "http://localhost:8080/api/games/search?name=射击"

# 响应示例
{
  "success": true,
  "count": 4,
  "data": [...]
}
```

#### 5.4 从 MySQL 获取所有游戏

```bash
curl http://localhost:8080/api/games/list

# 响应示例
{
  "success": true,
  "count": 15,
  "data": [...]
}
```

#### 5.5 从 Redis 获取单个游戏

```bash
curl http://localhost:8080/api/games/redis/1

# 响应示例
{
  "success": true,
  "data": {
    "id": 1,
    "name": "王者荣耀",
    "description": "5v5公平竞技手游",
    "category": "MOBA",
    "price": 0.0,
    ...
  }
}
```

#### 5.6 同步单个游戏

```bash
curl -X POST http://localhost:8080/api/games/sync/1

# 响应示例
{
  "success": true,
  "message": "Game synced to Redis successfully"
}
```

#### 5.7 重新创建搜索索引

```bash
curl -X POST http://localhost:8080/api/games/reindex

# 响应示例
{
  "success": true,
  "message": "Search index recreated successfully"
}
```

## 使用 Postman 测试

### 导入 Postman Collection

创建一个 Postman Collection 并添加以下请求：

1. **同步所有游戏**
   - Method: POST
   - URL: `http://localhost:8080/api/games/sync`

2. **搜索游戏**
   - Method: GET
   - URL: `http://localhost:8080/api/games/search?name=王者`

3. **获取所有游戏**
   - Method: GET
   - URL: `http://localhost:8080/api/games/list`

4. **从 Redis 获取游戏**
   - Method: GET
   - URL: `http://localhost:8080/api/games/redis/1`

5. **同步单个游戏**
   - Method: POST
   - URL: `http://localhost:8080/api/games/sync/1`

6. **重新创建索引**
   - Method: POST
   - URL: `http://localhost:8080/api/games/reindex`

## 高级用法

### 使用 Redis CLI 直接查询

```bash
# 进入 Redis CLI
docker exec -it redis-stack redis-cli

# 使用 Redis Search 进行查询
FT.SEARCH idx:game_list "@$.name:*王者*"

# 查询结果
1) (integer) 1
2) "game:1"
3) 1) "$"
   2) "{\"id\":1,\"name\":\"王者荣耀\",...}"

# 更复杂的查询
FT.SEARCH idx:game_list "@$.category:MOBA"
FT.SEARCH idx:game_list "@$.name:*联盟* | @$.name:*王者*"
```

### 性能测试

使用 Apache Bench 或其他压测工具测试性能：

```bash
# 安装 Apache Bench
sudo apt-get install apache2-utils  # Ubuntu/Debian
brew install apache-bench            # macOS

# 测试搜索性能
ab -n 1000 -c 10 "http://localhost:8080/api/games/search?name=王者"

# 测试同步性能
ab -n 100 -c 5 -p /dev/null -T "application/json" -m POST \
  "http://localhost:8080/api/games/sync/1"
```

## 故障排查

### 问题1：Redis 连接失败

**错误信息：**
```
Failed to create search index: Connection refused
```

**解决方案：**
1. 确认 Redis Stack 正在运行：
   ```bash
   docker ps | grep redis-stack
   ```

2. 检查 Redis 端口是否正确：
   ```bash
   telnet localhost 6379
   ```

3. 查看 application.yml 中的 Redis 配置是否正确

### 问题2：MySQL 连接失败

**错误信息：**
```
Communications link failure
```

**解决方案：**
1. 确认 MySQL 正在运行：
   ```bash
   docker ps | grep mysql
   ```

2. 测试 MySQL 连接：
   ```bash
   mysql -h localhost -u root -proot -e "SELECT 1"
   ```

3. 检查 application.yml 中的 MySQL 配置

### 问题3：搜索无结果

**可能原因：**
- 数据未同步到 Redis
- 搜索索引未创建
- 搜索关键词不匹配

**解决方案：**
1. 确认数据已同步：
   ```bash
   redis-cli KEYS game:*
   ```

2. 确认索引已创建：
   ```bash
   redis-cli FT.INFO idx:game_list
   ```

3. 重新创建索引：
   ```bash
   curl -X POST http://localhost:8080/api/games/reindex
   ```

4. 重新同步数据：
   ```bash
   curl -X POST http://localhost:8080/api/games/sync
   ```

### 问题4：中文搜索不准确

Redis Search 对中文的支持依赖于分词，默认按字符分词。

**改进方案：**
1. 使用短关键词搜索
2. 使用通配符 `*` 进行模糊匹配
3. 考虑集成中文分词器（需要额外配置）

## 下一步

### 功能扩展建议

1. **实时同步**
   - 使用 MySQL Binlog 监听实现增量同步
   - 使用 Spring Scheduling 定时同步

2. **更多搜索功能**
   - 价格范围搜索
   - 分类筛选
   - 排序功能
   - 分页查询

3. **性能优化**
   - 批量同步优化
   - Redis 连接池配置
   - 缓存预热

4. **监控和日志**
   - 添加 Prometheus 监控
   - 完善日志记录
   - 添加性能指标

5. **安全性**
   - API 认证和授权
   - 数据加密
   - 访问限流

## 相关资源

- [Redis Stack Documentation](https://redis.io/docs/stack/)
- [Redis JSON Commands](https://redis.io/docs/stack/json/)
- [Redis Search Guide](https://redis.io/docs/stack/search/)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [MyBatis-Plus Documentation](https://baomidou.com/)
