#include "testingsetup.h"
#include <boost/test/unit_test.hpp>
#include <iostream>
#include "json.hpp"
#include "testcurl.h"
#include <ctime>
#include <time.h>
#include <chrono>
using json = nlohmann::json;
static std::string s_main_url = "http://127.0.0.1:8080";
static std::string s_conversion_url = "http://127.0.0.1:6080";

static std::string postData(const std::string& method, const json& json_params)
{
        json json_post;
        json_post["id"] = 1000;
        json_post["method"] = method;
        json_post["params"] = json_params;
        return json_post.dump();
}

static uint64_t getCurrent()
{
    using namespace std::chrono;
    steady_clock::duration d = system_clock::now().time_since_epoch();
//	minutes min = duration_cast<minutes>(d);
//	seconds sec = duration_cast<seconds>(d);
	milliseconds mil = duration_cast<milliseconds>(d);
//	microseconds mic = duration_cast<microseconds>(d);
//	nanoseconds nan = duration_cast<nanoseconds>(d);
    //std::cout << min.count() << "分钟" << std::endl;
    //std::cout << sec.count() << "秒" << std::endl;
    //std::cout << mil.count() << "毫秒" << std::endl;
    //std::cout << mic.count() << "微妙" << std::endl;
    //std::cout << nan.count() << "纳秒" << std::endl;
    uint64_t clock_id = mil.count() ;
    std::cout  << "clock id : " << clock_id << std::endl;
    return clock_id;
}

static void getBalance(const std::string& asset_name, uint32_t uid)
{

}
BOOST_FIXTURE_TEST_SUITE(conversion,TestingSetup)

BOOST_AUTO_TEST_CASE(clear)
{
    /*
     * curl --data-binary
     * '{"id":1000, "method": "balance.query", "params": [3, "BTC"] }'
     *  -H 'content-type:text/plain;' http://127.0.0.1:8080
     */

    json json_params = json::array();
    json_params.push_back(3);
    json_params.push_back("BTC");

    CurlParams curl_params;
    curl_params.url = s_main_url;
    curl_params.data = postData("balance.query",json_params);
  
    std::string response;
    CurlPostParams(curl_params,response);
    std::cout << "response" << response << std::endl;
    json json_response = json::parse(response);
    /*{
    "error": null,
    "result": {
        "BTC": {
            "available": "10",
            "freeze": "0",
            "pledge": "0"
        }
    },
    "id": 1000
    }*/
    /*clock_t now_clock;
    now_clock = clock();
    std::cout << "clock :" << now_clock << std::endl;

    uint64_t clock_id = now_clock;
    std::cout << "clock id: " << clock_id << std::endl;
    */
    uint64_t now_time_id = getCurrent();
    std::cout << "current time: " << now_time_id << std::endl;
    time_t now_time;
    now_time = time(NULL);
    uint64_t time_id = now_time;
    std::cout  << "time id: " << time_id << std::endl;

    now_time_id = getCurrent();
    std::cout << "current time: " << now_time_id << std::endl;
    json json_asset = json_response["result"]["BTC"];
    std::string available = json_asset["available"].get<std::string>();
    std::string freeze = json_asset["freeze"].get<std::string>();
    std::string pledge = json_asset["pledge"].get<std::string>();
        
}


BOOST_AUTO_TEST_CASE(put_conversion)
{
    /*curl --data-binary '{"id":1000, "method": "conversion.put_conversion", "params": [1,3,
    "ETH", "BTC", "10", "0.5"] }' -H 'content-type:text/plain;' http://127.0.0.1:6080*/
    json json_params = json::array();



}

BOOST_AUTO_TEST_SUITE_END()
