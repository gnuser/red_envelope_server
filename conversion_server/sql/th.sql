
-- split by user_id
CREATE TABLE `balance_history_example` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `asset` varchar(30) NOT NULL,
  `business` varchar(30) NOT NULL,
  `change` decimal(30,8) NOT NULL,
  `balance` decimal(30,16) NOT NULL,
  `detail` text NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_asset` (`user_id`,`asset`),
  KEY `idx_user_asset_business` (`user_id`,`asset`,`business`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- split by user_id
CREATE TABLE `user_deal_history_example` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `market` varchar(30) NOT NULL,
  `deal_id` bigint(20) unsigned NOT NULL,
  `order_id` bigint(20) unsigned NOT NULL,
  `deal_order_id` bigint(20) unsigned NOT NULL,
  `side` tinyint(3) unsigned NOT NULL,
  `role` tinyint(3) unsigned NOT NULL,
  `price` decimal(30,9) NOT NULL,
  `amount` decimal(30,8) NOT NULL,
  `deal` decimal(30,16) NOT NULL,
  `fee` decimal(30,16) NOT NULL,
  `deal_fee` decimal(30,16) NOT NULL,
  `token` varchar(30) NOT NULL,
  `token_rate` decimal(30,8) NOT NULL,
  `asset_rate` decimal(30,8) NOT NULL,
  `discount` decimal(30,4) NOT NULL,
  `deal_token` decimal(30,16) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_market` (`user_id`,`market`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- split by user_id
CREATE TABLE `order_history_example` (
  `id` bigint(20) unsigned NOT NULL,
  `create_time` double NOT NULL,
  `finish_time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `market` varchar(30) NOT NULL,
  `source` varchar(30) NOT NULL,
  `t` tinyint(3) unsigned NOT NULL,
  `side` tinyint(3) unsigned NOT NULL,
  `price` decimal(30,9) NOT NULL,
  `amount` decimal(30,8) NOT NULL,
  `taker_fee` decimal(30,4) NOT NULL,
  `maker_fee` decimal(30,4) NOT NULL,
  `deal_stock` decimal(30,8) NOT NULL,
  `deal_money` decimal(30,16) NOT NULL,
  `deal_fee` decimal(30,16) NOT NULL,
  `token` varchar(30) NOT NULL,
  `token_rate` decimal(30,8) NOT NULL,
  `asset_rate` decimal(30,8) NOT NULL,
  `discount` decimal(30,4) NOT NULL,
  `deal_token` decimal(30,16) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_market` (`user_id`,`market`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- split by id, aka orer_id
CREATE TABLE `order_detail_example` (
  `id` bigint(20) unsigned NOT NULL,
  `create_time` double NOT NULL,
  `finish_time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `market` varchar(30) NOT NULL,
  `source` varchar(30) NOT NULL,
  `t` tinyint(3) unsigned NOT NULL,
  `side` tinyint(3) unsigned NOT NULL,
  `price` decimal(30,9) NOT NULL,
  `amount` decimal(30,8) NOT NULL,
  `taker_fee` decimal(30,4) NOT NULL,
  `maker_fee` decimal(30,4) NOT NULL,
  `deal_stock` decimal(30,8) NOT NULL,
  `deal_money` decimal(30,16) NOT NULL,
  `deal_fee` decimal(30,16) NOT NULL,
  `token` varchar(30) NOT NULL,
  `token_rate` decimal(30,8) NOT NULL,
  `asset_rate` decimal(30,8) NOT NULL,
  `discount` decimal(30,4) NOT NULL,
  `deal_token` decimal(30,16) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_market` (`user_id`,`market`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- split by order_id
CREATE TABLE `deal_history_example` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `deal_id` bigint(20) unsigned NOT NULL,
  `order_id` bigint(20) unsigned NOT NULL,
  `deal_order_id` bigint(20) unsigned NOT NULL,
  `role` tinyint(3) unsigned NOT NULL,
  `price` decimal(30,9) NOT NULL,
  `amount` decimal(30,8) NOT NULL,
  `deal` decimal(30,16) NOT NULL,
  `fee` decimal(30,16) NOT NULL,
  `deal_fee` decimal(30,16) NOT NULL,
  `token` varchar(30) NOT NULL,
  `token_rate` decimal(30,8) NOT NULL,
  `asset_rate` decimal(30,8) NOT NULL,
  `discount` decimal(30,4) NOT NULL,
  `deal_token` decimal(30,16) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

