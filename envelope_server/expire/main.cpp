#include <ev.h>
#include <stdio.h>
#include <iostream>
#include <curl/curl.h>
#include "json.hpp"

using json = nlohmann::json;
std::string url = "http://127.0.0.1:5080";
int time_span = 60 * 5;

struct ev_loop *loop = EV_DEFAULT;
ev_timer timer_watcher;

struct CurlParams
{
    std::string curl_url;
    std::string curl_post_data;
    std::string curl_response;
};

static size_t ReplyCallback(void *ptr, size_t size, size_t nmemb, void *stream)
{
    std::string *str = (std::string*)stream;
    (*str).append((char*)ptr, size*nmemb);
    return size * nmemb;
}

bool CurlPost(CurlParams *curl_params)
{
    CURL *curl = curl_easy_init();
    struct curl_slist *headers = NULL;
    CURLcode res;
    curl_params->curl_response.clear();

    std::string error_str ;
    if (curl)
    {
        headers = curl_slist_append(headers, "content-type:text/plain;");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_URL, curl_params->curl_url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)curl_params->curl_post_data.size());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curl_params->curl_post_data.c_str());

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ReplyCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&curl_params->curl_response);
        curl_easy_setopt(curl, CURLOPT_USE_SSL, CURLUSESSL_TRY);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30);
        res = curl_easy_perform(curl);
    }
    curl_easy_cleanup(curl);

    if (res != CURLE_OK)
    {
        error_str = curl_easy_strerror(res);
        std::cout << "error: " << error_str << std::endl;
        return false;
    }
    return true;
}

bool order_book()
{
    CurlParams* curl_book = new CurlParams();
    json json_book;
    json_book["id"] = 100;
    json_book["method"] = "order.book";
    json json_params = json::array();
    json_params.push_back(1);
    json_book["params"] = json_params;
    curl_book->curl_url = url;
    curl_book->curl_post_data = json_book.dump(0);
    CurlPost(curl_book);

    if (curl_book->curl_response.size() == 0) 
        return false;

    json res_book = json::parse(curl_book->curl_response);
    if (res_book.find("result") == res_book.end())
        return false;
        
    std::cout << "order.book: " << res_book.dump(4) << std::endl;
    json orders = res_book["result"]["records"];
    if (orders.size() > 0)
    {
        uint64_t order_id = orders.at(0)["id"].get<uint64_t>();
        std::cout << "order_id: " << order_id << std::endl;
        CurlParams* curl_cancel = new CurlParams();
        json json_cancel;
        json_cancel["id"] = 200;
        json_cancel["method"] = "order.cancel";
        curl_cancel->curl_url = url;
        json json_params = json::array();
        json_params.push_back(order_id);
        json_cancel["params"] = json_params;
        curl_cancel->curl_post_data = json_cancel.dump(0);
        CurlPost(curl_cancel);
        std::cout << "order.cancel: " << curl_cancel->curl_response << std::endl;
    }
}

void timer_action(struct ev_loop *main_loop, ev_timer*time_w, int e)
{
    order_book();
}

int main(int argc, char *argv[])
{
    std::cout << "url: " << url << std::endl;
    ev_init(&timer_watcher, timer_action);
    ev_timer_set(&timer_watcher, time_span, time_span);
    ev_timer_start(loop, &timer_watcher);
    ev_run(loop,0);
    return 0;
}

