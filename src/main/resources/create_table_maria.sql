CREATE TABLE IF NOT EXISTS `profiles` (
  `sid` bigint(20) NOT NULL AUTO_INCREMENT,
  `uid` bigint(20) NOT NULL,
  `user_name` tinytext NOT NULL,
  `profile` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  PRIMARY KEY (`sid`),
  UNIQUE KEY `uid_idx` (`uid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4;
