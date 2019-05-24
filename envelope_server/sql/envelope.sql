
CREATE TABLE `user_envelope_history` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `asset` varchar(30) NOT NULL,
  `envelope_id` bigint(20) unsigned NOT NULL,
  `role` tinyint(3) unsigned NOT NULL,
  `amount` varchar(30) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `envelope_detail` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `time` double NOT NULL,
  `envelope_id` bigint(20) unsigned NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `asset` varchar(30) NOT NULL,
  `type` tinyint(3) unsigned NOT NULL,
  `supply` varchar(30) NOT NULL,
  `share` int(10) NOT NULL,
  `expire_time` int(10) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_envelope_example` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `time` double NOT NULL,
  `envelope_id` bigint(20) unsigned NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `asset` varchar(30) NOT NULL,
  `type` tinyint(3) unsigned NOT NULL,
  `supply` varchar(30) NOT NULL,
  `leave` varchar(30) NOT NULL,
  `share` int(10) NOT NULL,
  `expire_time` int(10) NOT NULL,
  `count` int(10) NOT NULL,
  `history` MEDIUMTEXT NOT NULL,	
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_history` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          BIGINT NOT NULL,
    `end_oper_id`   BIGINT UNSIGNED NOT NULL,
    `end_order_id`  BIGINT UNSIGNED NOT NULL,
    `end_deals_id`  BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `operlog_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `time`          DOUBLE NOT NULL,
    `detail`        TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


