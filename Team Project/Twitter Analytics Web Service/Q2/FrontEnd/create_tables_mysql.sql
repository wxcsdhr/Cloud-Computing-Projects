innodb_buffer_pool_size = 4G

USE phase1;
DROP TABLE IF EXISTS `tweets`;
CREATE TABLE `tweets` (
  `userId` varchar(64) NOT NULL,
  `content` blob NOT NULL,
  `wordCount` varchar(1024) NULL,
  PRIMARY KEY (`userId`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

LOAD DATA LOCAL INFILE '/home/ubuntu/part-00000' INTO TABLE tweets FIELDS TERMINATED BY '#';

CREATE INDEX idxUserId ON tweets (userId);