# QuickTrade Envelope Server

## Code structure

**Required systems**

* MySQL: For saving operation log, user balance history, order history and trade history.

**Base library**

* network: An event base and high performance network programming library, easily supporting 1000K TCP connections. Include TCP/UDP/UNIX SOCKET server and client implementation, a simple timer, state machine, thread pool. 

* utils: Some basic library, including log, config parse, some data structure and http/websocket/rpc server implementation.

**Modules**

* matchengine: This is the most important part for it records user balance and executes user order. It is in memory database, saves operation log in MySQL and redoes the operation log when start. It also writes user history into MySQL, push balance, orders and deals message to kafka.

* readhistory: Reads history data from MySQL.

* accesshttp: Supports a simple HTTP interface and hides complexity for upper layer.

* alertcenter: A simple server that writes FATAL level log to redis list so we can send alert emails.

## Compile and Install

**Operating system**

Ubuntu 14.04 or Ubuntu 16.04. Not yet tested on other systems.

**Requirements**

See [requirements](https://github.com/viabtc/viabtc_exchange_server/wiki/requirements). Install the mentioned system or library.

[libev](https://github.com/paizzj/libev)

You MUST use the depends/hiredis to install the hiredis library. Or it may not be compatible.

**Compilation**

Compile network and utils first. The rest all are independent.

```
matchengine
|---bin
|   |---matchengine.exe
|---log
|   |---matchengine.log
|---conf
|   |---config.json
|---shell
|   |---restart.sh
|   |---check_alive.sh
```


