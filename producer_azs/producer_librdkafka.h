#pragma once

#include "../shared_structures.h"
#include <iostream>

#include <string>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <mutex>
#include <atomic>
#include <json/json.h>


class LibrdKafkaProducer
{
    public:
        LibrdKafkaProducer() = default;
        ~LibrdKafkaProducer();

        bool init(const std::string& brokers, const std::string& topic);
        bool flush(int timeout_ms);
        bool sendMessage(const std::string& key, const std::string& value);
        bool sendJson(const Json::Value& j, const std::string& key = "");
        ProducerStats getStats() const;


    private:
        std::unique_ptr<RdKafka::Producer> producer_;
        std::unique_ptr<RdKafka::Topic> topic_;
        std::string topic_name_;

        std::atomic<bool> stopping_{false}; // ???
        ProducerStats stats_;
        mutable std::mutex stats_mutex_;

        class DeliveryCb : public RdKafka::DeliveryReportCb
        {
            public:
                void dr_cb(RdKafka::Message& message) override;
                LibrdKafkaProducer* producer_{nullptr};
        };

        DeliveryCb delivery_cb_;
};
