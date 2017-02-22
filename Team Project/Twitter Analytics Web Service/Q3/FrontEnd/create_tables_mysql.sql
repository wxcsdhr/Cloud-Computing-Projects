-- innodb_buffer_pool_size = 4G

USE phase3;
DROP TABLE IF EXISTS `q3tweets`;
CREATE TABLE `q3tweets` (
  `tweetId` bigint(32) NOT NULL,
  `createAt` bigint(16) NOT NULL,
  `userId` bigint(32) NOT NULL,
  `wc` varchar(1024) NOT NULL,
  PRIMARY KEY (`tweetId`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
