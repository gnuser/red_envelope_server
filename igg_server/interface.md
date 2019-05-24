
资产在主撮合保存
兑换记录在子撮合保存

兑换撮合
接口: put_conversion
{
	调用形式:
	put_conversion(role,user_id,stock,money,volume,price) --> maker调用形式
    put_conversion(role,user_id,stock,money,volume,conversion_id) --->toker调用形式
	curl 调用举例:
	maker:
	curl --data-binary '{"id":1000, "method": "conversion.put_conversion", "params": [1,3, "ETH", "BTC", "10", "0.5"] }' -H 'content-type:text/plain;' http://127.0.0.1:6080
	taker:
	curl --data-binary '{"id":1000, "method": "conversion.put_conversion", "params": [2,4, "ETH", "BTC", "2", 1] }' -H 'content-type:text/plain;' http://127.0.0.1:6080
	
}

参数：	
role:代表这次请求的角色
{ 
1是maker,2是taker
}
user_id:用户id
{
	发起请求的用户id
}
stock:股票
{
	
}
volume:股票数量
{
	maker中表示需要冻结的股票的数量，
	taker中表示兑换的股票的数量

money:货币
{
	
}
price:兑换价格
{
	仅对maker有意义。
}
conversion_id:兑换的订单id
{
	仅对taker有意义。
}
}

maker 发起兑换
put_conversion(role,user_id,stock,money,volume,price)
向主撮合发起冻结资产操作(plege接口)，
实现对volume数量的stock冻结，
并在兑换撮合生成订单id为conversion_id的兑换订单返回给maker。

taker 撮合兑换
put_conversion(role,user_id,volume,conversion_id)
在兑换撮合完成对订单(order)id为conversion_id的兑换，
首先taker要冻结相应的货币:
taker 冻结volume数量对应价格的money (pledge +(order->price * volume))
同时向主撮合进行资产更新:
maker 提取部分stock数量(withdraw -),添加部分money数量(balance update +)
taker 提取部分money的数量(withdraw -),添加部分stock数量(balance update +)
完整过程举例:

maker:
request(用户---->兑换撮合):
put_conversion(1,11,ETH,BTC,0.5,10)

request(兑换撮合---->主撮合):
plege(11,ETH,10)

reply(主撮合----->兑换撮合):
成功或者失败

reply(兑换撮合---->用户)：
成功则返回conversion_id(这里假设是123456)，否则返回失败

taker:
request(用户---->兑换撮合):
put_conversion(1,12,123456,5)

request(兑换撮合---->主撮合):
withdraw(11,ETH,5)
balance_update(11,BTC,2.5)
balance_update(12,ETH,5)
balance_update(12,BTC,-2.5)

reply(兑换撮合---->用户):
成功或者失败


