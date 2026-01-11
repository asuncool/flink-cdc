#!/bin/bash

# MySQL to Redis JSON 同步环境快速设置脚本

echo "================================"
echo "MySQL to Redis JSON 环境设置"
echo "================================"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo -e "${RED}错误: Docker 未安装，请先安装 Docker${NC}"
    echo "访问 https://docs.docker.com/get-docker/ 获取安装指南"
    exit 1
fi

echo -e "${GREEN}✓ Docker 已安装${NC}"
echo ""

# 1. 启动 MySQL
echo "================================"
echo "1. 启动 MySQL 容器"
echo "================================"

if docker ps -a | grep -q "mysql-gamedb"; then
    echo -e "${YELLOW}MySQL 容器已存在，正在停止并删除...${NC}"
    docker stop mysql-gamedb 2>/dev/null
    docker rm mysql-gamedb 2>/dev/null
fi

echo "启动 MySQL 容器..."
docker run -d \
  --name mysql-gamedb \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=gamedb \
  mysql:8.0

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ MySQL 容器启动成功${NC}"
else
    echo -e "${RED}✗ MySQL 容器启动失败${NC}"
    exit 1
fi

# 等待 MySQL 启动
echo "等待 MySQL 启动..."
sleep 15

# 2. 初始化数据库
echo ""
echo "================================"
echo "2. 初始化数据库"
echo "================================"

if [ -f "src/main/resources/sql/init_game_list.sql" ]; then
    echo "执行初始化脚本..."
    docker exec -i mysql-gamedb mysql -uroot -proot < src/main/resources/sql/init_game_list.sql
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 数据库初始化成功${NC}"
    else
        echo -e "${RED}✗ 数据库初始化失败${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ 找不到初始化脚本: src/main/resources/sql/init_game_list.sql${NC}"
    exit 1
fi

# 验证数据
echo "验证数据..."
GAME_COUNT=$(docker exec mysql-gamedb mysql -uroot -proot -e "SELECT COUNT(*) FROM gamedb.game_list" -s -N 2>/dev/null)
echo -e "${GREEN}✓ 游戏数据已插入：${GAME_COUNT} 条记录${NC}"

# 3. 启动 Redis Stack
echo ""
echo "================================"
echo "3. 启动 Redis Stack 容器"
echo "================================"

if docker ps -a | grep -q "redis-stack-gamedb"; then
    echo -e "${YELLOW}Redis Stack 容器已存在，正在停止并删除...${NC}"
    docker stop redis-stack-gamedb 2>/dev/null
    docker rm redis-stack-gamedb 2>/dev/null
fi

echo "启动 Redis Stack 容器..."
docker run -d \
  --name redis-stack-gamedb \
  -p 6379:6379 \
  -p 8001:8001 \
  redis/redis-stack:latest

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Redis Stack 容器启动成功${NC}"
else
    echo -e "${RED}✗ Redis Stack 容器启动失败${NC}"
    exit 1
fi

# 等待 Redis 启动
echo "等待 Redis Stack 启动..."
sleep 5

# 测试 Redis 连接
echo "测试 Redis 连接..."
docker exec redis-stack-gamedb redis-cli PING > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Redis 连接测试成功${NC}"
else
    echo -e "${RED}✗ Redis 连接测试失败${NC}"
    exit 1
fi

# 4. 总结
echo ""
echo "================================"
echo "环境设置完成！"
echo "================================"
echo ""
echo "容器信息："
echo "  MySQL:"
echo "    - 容器名称: mysql-gamedb"
echo "    - 端口: 3306"
echo "    - 用户名: root"
echo "    - 密码: root"
echo "    - 数据库: gamedb"
echo ""
echo "  Redis Stack:"
echo "    - 容器名称: redis-stack-gamedb"
echo "    - Redis 端口: 6379"
echo "    - RedisInsight 端口: 8001"
echo "    - RedisInsight 访问: http://localhost:8001"
echo ""
echo "下一步："
echo "  1. 启动 Spring Boot 应用:"
echo "     mvn spring-boot:run"
echo ""
echo "  2. 同步数据到 Redis:"
echo "     curl -X POST http://localhost:8080/api/games/sync"
echo ""
echo "  3. 测试搜索功能:"
echo "     curl \"http://localhost:8080/api/games/search?name=王者\""
echo ""
echo "  4. 查看 RedisInsight (可选):"
echo "     在浏览器中打开 http://localhost:8001"
echo ""
echo "停止环境:"
echo "  ./scripts/stop-env.sh"
echo ""
echo -e "${GREEN}✓ 所有设置完成！${NC}"
