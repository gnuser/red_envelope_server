#include "testingsetup.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <boost/test/unit_test.hpp>
#include <fstream>
#include "json.hpp"
//#include "testcurl.h"
using json = nlohmann::json;

static std::string s_single_file  = "../single.json";
static std::string s_mutil_file = "../mutil.json";

BOOST_FIXTURE_TEST_SUITE(io,TestingSetup)

BOOST_AUTO_TEST_CASE(example)
{
    std::ofstream jfile(s_single_file);
    json json_single;
    json_single["url"] = "http://127.0.0.1:6080";
    json json_post;

    //'{"id":1000, "method": "balance.query", "params": [3, "BTC"]}'
     json_post["id"] = 1000;
     json_post["method"] = "balance.query";
     json json_params = json::array();
     json_params.push_back(3);
     json_params.push_back("BTC");
     json_post["params"] = json_params;
     json_single["post"] = json_post;
     json_single >> jfile;
     jfile.close();

     std::ofstream
}


BOOST_AUTO_TEST_CASE(single)
{
/*    std::ifstream jfile(s_single_file);
    if (!jfile)
    {
        std::cout << "No " << s_single_file << " file!" << std::endl;
        return;
    }

    json json_single;
    jfile >> json_single;
    if(json_single.is_object())
    {
        std::cout << s_single_file << "is not json file" << std::endl;
        jfile.close();
        return;
    }

    std::string rpc_url = json_single["url"].get<std::string>();

    json json_post = json_single["post"].get<std::string>();

    CurlParams curl_params;
    curl_params.url = rpc_url;
    curl_params.data = json_post.dump();

    std::string response;
//    CurlPostParams(curl_params,response);
    std::cout << "response" << response << std::endl;
    json json_response = json::parse(response);
*/
}

BOOST_AUTO_TEST_CASE(c_test)
{
	printf("This is C io test!\n");
	printf("begin ------------------------\n");
	printf("end --------------------------\n");

}

BOOST_AUTO_TEST_CASE(make_test)
{
	std::cout << "This is make test!" << std::endl;
	std::cout << "begin ---------------------------" << std::endl;
	std::cout << "end -----------------------------" << std::endl;
}

BOOST_AUTO_TEST_SUITE_END()
