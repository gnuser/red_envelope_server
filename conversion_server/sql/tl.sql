
CREATE TABLE `slice_balance_example` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `user_id`       INT UNSIGNED NOT NULL,
    `asset`         VARCHAR(30) NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL,
    `balance`       DECIMAL(30,16) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_order_example` (
  `id` bigint(20) unsigned NOT NULL,
  `t` tinyint(3) unsigned NOT NULL,
  `side` tinyint(3) unsigned NOT NULL,
  `create_time` double NOT NULL,
  `update_time` double NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `market` varchar(30) NOT NULL,
  `price` decimal(30,9) NOT NULL,
  `amount` decimal(30,8) NOT NULL,
  `taker_fee` decimal(30,4) NOT NULL,
  `maker_fee` decimal(30,4) NOT NULL,
  `left` decimal(30,8) NOT NULL,
  `freeze` decimal(30,8) NOT NULL,
  `deal_stock` decimal(30,8) NOT NULL,
  `deal_money` decimal(30,16) NOT NULL,
  `deal_fee` decimal(30,12) NOT NULL,
  `token` varchar(30) NOT NULL,
  `token_rate` decimal(30,8) NOT NULL,
  `asset_rate` decimal(30,8) NOT NULL,
  `discount` decimal(30,4) NOT NULL,
  `deal_token` decimal(30,16) NOT NULL,
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

