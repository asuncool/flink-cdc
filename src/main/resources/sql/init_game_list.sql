-- 创建数据库
CREATE DATABASE IF NOT EXISTS gamedb DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE gamedb;

-- 创建游戏列表表
CREATE TABLE IF NOT EXISTS game_list (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '游戏ID',
    name VARCHAR(255) NOT NULL COMMENT '游戏名称',
    description TEXT COMMENT '游戏描述',
    category VARCHAR(100) COMMENT '游戏分类',
    price DECIMAL(10, 2) COMMENT '游戏价格',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_name (name),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='游戏列表表';

-- 插入示例数据
INSERT INTO game_list (name, description, category, price) VALUES
('王者荣耀', '5v5公平竞技手游', 'MOBA', 0.00),
('和平精英', '腾讯光子自研反恐军事竞赛体验手游', '射击', 0.00),
('原神', '开放世界冒险游戏', 'RPG', 0.00),
('英雄联盟', '多人在线战术竞技游戏', 'MOBA', 0.00),
('穿越火线', '第一人称射击游戏', '射击', 0.00),
('我的世界', '沙盒建造游戏', '沙盒', 68.00),
('DNF', '动作角色扮演游戏', 'ARPG', 0.00),
('梦幻西游', '回合制角色扮演游戏', 'RPG', 0.00),
('炉石传说', '集换式卡牌游戏', '卡牌', 0.00),
('DOTA2', '多人在线战术竞技游戏', 'MOBA', 0.00),
('CS:GO', '第一人称射击游戏', '射击', 88.00),
('守望先锋', '团队射击游戏', '射击', 128.00),
('塞尔达传说', '动作冒险游戏', '冒险', 299.00),
('超级马里奥', '横版跳跃游戏', '平台', 298.00),
('魔兽世界', '大型多人在线角色扮演游戏', 'MMORPG', 75.00);
