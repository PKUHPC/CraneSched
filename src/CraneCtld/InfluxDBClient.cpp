#include "InfluxDBClient.h"
#include <curl/curl.h>
#include <sstream>

// 用于处理HTTP响应的回调函数
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

bool InfluxDBClient::Write(const std::string& bucket, const std::string& org,
                          const std::string& measurement, const std::string& field,
                          double value) const {
    CURL* curl;
    CURLcode res;
    
    curl = curl_easy_init();
    if(curl) {
        std::string url = m_url + "/api/v2/write?bucket=" + bucket + "&org=" + org;
        
        // 构建行协议数据
        std::stringstream data;
        data << measurement << " " << field << "=" << value;
        std::string lineProtocol = data.str();

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, lineProtocol.c_str());

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");
        headers = curl_slist_append(headers, ("Authorization: Token " + m_token).c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        res = curl_easy_perform(curl);
        
        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
        
        return res == CURLE_OK;
    }
    return false;
}

std::string InfluxDBClient::Query(const std::string& flux_query) const {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl = curl_easy_init();
    if(curl) {
        std::string url = m_url + "/api/v2/query?org=" + m_org;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, flux_query.c_str());

        // 设置HTTP头
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/vnd.flux");
        headers = curl_slist_append(headers, ("Authorization: Token " + m_token).c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            // 处理错误
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        }

        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
    }
    return readBuffer;
} 