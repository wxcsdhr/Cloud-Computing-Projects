innodb_buffer_pool_size = 4G

SET bulk_insert_buffer_size = 1024 * 1024 * 1024;
SET myisam_sort_buffer_size = 1024 * 1024 * 1024;
SET GLOBAL key_buffer_size = 1024 * 1024 * 1024;

USE phase1;
DROP TABLE IF EXISTS `tweets`;
CREATE TABLE `tweets` (
  `userId` varchar(64) NOT NULL,
  `content` mediumblob NOT NULL,
  `wordCount` mediumtext NULL,
  PRIMARY KEY (`userId`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

LOAD DATA LOCAL INFILE '/home/ubuntu/dataset/merged' INTO TABLE tweets FIELDS TERMINATED BY '#';

CREATE INDEX idxUserId ON tweets (userId);