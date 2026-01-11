#!/bin/bash

# 停止环境脚本

echo "================================"
echo "停止 MySQL to Redis JSON 环境"
echo "================================"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 停止 MySQL
echo "停止 MySQL 容器..."
if docker ps | grep -q "mysql-gamedb"; then
    docker stop mysql-gamedb
    echo -e "${GREEN}✓ MySQL 容器已停止${NC}"
else
    echo -e "${YELLOW}! MySQL 容器未运行${NC}"
fi

# 停止 Redis Stack
echo "停止 Redis Stack 容器..."
if docker ps | grep -q "redis-stack-gamedb"; then
    docker stop redis-stack-gamedb
    echo -e "${GREEN}✓ Redis Stack 容器已停止${NC}"
else
    echo -e "${YELLOW}! Redis Stack 容器未运行${NC}"
fi

echo ""
echo "如需完全删除容器，请运行:"
echo "  docker rm mysql-gamedb redis-stack-gamedb"
echo ""
echo "如需重新启动，请运行:"
echo "  docker start mysql-gamedb redis-stack-gamedb"
echo ""
echo -e "${GREEN}✓ 环境已停止${NC}"
