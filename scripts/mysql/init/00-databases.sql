-- 官方 MySQL 镜像只对新数据卷运行初始化文件。UC 账号键区分大小写。
CREATE DATABASE IF NOT EXISTS slg_uc
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
