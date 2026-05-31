#include "consumer_librdkafka.h"

#include <iostream>
#include <vector>
#include <thread>


LibRdkafkaConsumer::LibRdkafkaConsumer()  : message_callback_(nullptr), error_callback_(nullptr)
{
    conninfo_ = ConnInfo();
    stats_ = ConsumerStats();
    if(!init(conninfo_.brokers_names, conninfo_.topic, conninfo_.group_id))
    {
        throw  std::runtime_error("Failed to init consumer");
    }
}

LibRdkafkaConsumer::~LibRdkafkaConsumer()
{
    stop();
}

const bool LibRdkafkaConsumer::status() const
{
    return running_;
}


bool LibRdkafkaConsumer::init(const std::string& brokers, const std::string& topic, const std::string &group_id)
{
    topic_name_ = topic;
    std::string errstr;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    tconf->set("auto.offset.reset", "earliest", errstr);
    if (conf->set("default_topic_conf", tconf, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set default_topic_conf: " << errstr << std::endl;
    }

    
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("broker.address.family", "v4", errstr);
    conf->set("group.id", group_id, errstr);
    conf->set("auto.offset.reset", "earliest", errstr);
    conf->set("enable.auto.commit", "true", errstr);
    conf->set("auto.commit.interval.ms", "5000", errstr);
    conf->set("session.timeout.ms", "45000", errstr);
    conf->set("max.poll.interval.ms", "300000", errstr);
    // conf->set("api.version.request", "true", errstr);
    // conf->set("api.version.fallback.ms", "0", errstr);


    consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    delete conf;
    delete tconf;

    if(!consumer_){
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return false;
    }

    std::vector<std::string> topics = {topic};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if(err != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Failed to subscribe to " << topic << ": "  << RdKafka::err2str(err) << std::endl;
        return false;
    }

    std::cout << "Kafka consumer initialized for " << brokers << " topic " << topic << " group " << group_id << std::endl;
    running_ = true;
    return true;
}

void LibRdkafkaConsumer::preStart()
{
    if(!consumer_)
    {
        throw std::runtime_error("Consumer not initialized");
    }

    consumer_->poll(0);
    
    running_ = true;
    
    std::cout << "Started consuming from " << topic_name_ << std::endl;
}

void LibRdkafkaConsumer::startConsuming()
{
    // if(!consumer_)
    // {
    //     throw std::runtime_error("Consumer not initialized");
    //     // return;
    // }

    // consumer_->poll(0);

    // running_ = true;
    // std::cout << "Started consuming from " << topic_name_ << std::endl;

    // while(running_)
    // {
        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000));
        switch(msg->err())
        {
            case RdKafka::ERR_NO_ERROR:
                {
                    std::string payload(static_cast<const char*>(msg->payload()), msg->len());
                    std::string key = msg->key() ? *msg->key() : "";
                    {
                        std::lock_guard<std::mutex> lock(stats_mutex_);
                        stats_.message_received++;
                        stats_.bytes_received += msg->len();
                    }

                    Json::Value root;
                    Json::CharReaderBuilder reader;
                    std::string errs;
                    std::istringstream iss(payload);

                    if(Json::parseFromStream(reader, iss, &root, &errs))
                    {
                        if(message_callback_)
                        {
                            message_callback_(root, key);
                        } else{
                                std::cout << "Received not saved message [key: " << key << "]:\n";
                                Json::StreamWriterBuilder writer;
                                writer["indentation"] = "  ";
                                std::cout << Json::writeString(writer, root) << std::endl;
                            }
                    } else{
                        if(error_callback_)
                        {
                            std::stringstream ss;
                            ss << "Failed to parse JSON: " << errs << "Raw payload: " << payload;
                            error_callback_(ss.str());
                        }
                    }
                    break;
                }
            case RdKafka::ERR__TIMED_OUT:
                break;
            case RdKafka::ERR__PARTITION_EOF:
                std::cout << "New data is over" << std::endl;
                break;
            default:
                if(error_callback_)
                {
                    error_callback_(msg->errstr());
                }

                {
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    stats_.errors++;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                break;
        }
}

void LibRdkafkaConsumer::stop()
{
    running_ = false;
    if(consumer_) {
        consumer_->close();
    }
}


void LibRdkafkaConsumer::setMessageCallback(std::function<void(const Json::Value&, const std::string&)> cb)
{
    message_callback_ = cb;
}

void LibRdkafkaConsumer::setErrorCallback(std::function<void(const std::string&)> cb)
{
    error_callback_ = cb;
}