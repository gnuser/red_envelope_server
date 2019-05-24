/*
 * Description: 
 *     History: chenfei, 2018/12/17, create
 */

# include "me_redis.h"
# include "me_config.h"

static redis_sentinel_t *redis;

static int load_market_last(redisContext *context)
{
	for (size_t i = 0; i < settings.market_num; ++i) {
        if (settings.markets[i].name == NULL || strlen(settings.markets[i].name) == 0)
            continue;

        redisReply *reply = redisCmd(context, "GET k:%s:last", settings.markets[i].name);
        if (reply == NULL) {
            return -__LINE__;
        }

        if (reply->type == REDIS_REPLY_STRING) {
            settings.markets[i].last = decimal(reply->str, 0);
            if (settings.markets[i].last == NULL) {
                freeReplyObject(reply);
                return -__LINE__;
            }
        } else {
            // set default value = 0
            settings.markets[i].last = decimal("0", 0);
        }
        freeReplyObject(reply);
    }
    return 0;
}

int init_redis()
{
	redis = redis_sentinel_create(&settings.redis);
    if (redis == NULL)
        return -__LINE__;

    redisContext *context = redis_sentinel_connect_master(redis);
    if (context == NULL)
        return -__LINE__;

    return load_market_last(context);    
}

