-- innodb_buffer_pool_size = 4G

USE phase3;
DROP TABLE IF EXISTS `q4tweets`;
CREATE TABLE `q4tweets` (
  `tweetid` bigint(32) NOT NULL,
  `timestamp` bigint(16),
  `hashtag` varchar(4096),
  `userid` bigint(32),
  `username` varchar(4096),
  `text` varchar(4096),
  PRIMARY KEY (`tweetid`)
) ENGINE=Innodb DEFAULT CHARSET=utf8;
