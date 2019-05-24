
CREATE TABLE `system_coin_type` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `short_name` varchar(16) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin; 

CREATE TABLE `system_trade_type` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `buy_coin_id` int(11) unsigned DEFAULT NULL,
  `sell_coin_id` int(11) unsigned DEFAULT NULL,
  `min_count` decimal(24,4) DEFAULT NULL,
  `status` int(11) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
