/*
 * Description: 
 *     History: yang@haipo.me, 2016/09/30, create
 */

# ifndef _UT_RPC_CMD_H_
# define _UT_RPC_CMD_H_

// balance
# define CMD_BALANCE_QUERY          101
# define CMD_BALANCE_UPDATE         102
# define CMD_BALANCE_HISTORY        103
# define CMD_ASSET_LIST             104
# define CMD_ASSET_SUMMARY          105

# define CMD_BALANCE_FREEZE         106
# define CMD_PLEDGE_WITHDRAW        107

// trade
# define CMD_ORDER_PUT_LIMIT        201
# define CMD_ORDER_PUT_MARKET       202
# define CMD_ORDER_QUERY            203
# define CMD_ORDER_CANCEL           204
# define CMD_ORDER_BOOK             205
# define CMD_ORDER_BOOK_DEPTH       206
# define CMD_ORDER_DETAIL           207
# define CMD_ORDER_HISTORY          208
# define CMD_ORDER_DEALS            209
# define CMD_ORDER_DETAIL_FINISHED  210
# define CMD_ORDER_CANCEL_BATCH     211

// market
# define CMD_MARKET_USER_DEALS      306
# define CMD_MARKET_LIST            307
# define CMD_MARKET_SUMMARY         308

# define CMD_MARKET_UPDATE          401
# define CMD_ASSET_UPDATE           402
# define CMD_MARKET_DEPTH           405
# define CMD_USER_ORDER_HISTORY     406

// envelope
# define CMD_ENVELOPE_PUT           501
# define CMD_ENVELOPE_OPEN          502
# define CMD_ENVELOPE_HISTORY       503
# define CMD_ENVELOPE_DETAIL        504

# endif

