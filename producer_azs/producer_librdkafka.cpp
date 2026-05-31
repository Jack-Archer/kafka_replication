#include "producer_librdkafka.h"

#include <librdkafka/rdkafkacpp.h>


LibrdKafkaProducer::~LibrdKafkaProducer()
{
    stopping_ = true;

    if(producer_)
    {
        producer_->flush(10000);
    }

    producer_.reset();
}


bool LibrdKafkaProducer::init(const std::string& brokers, const std::string& topic)
{
    topic_name_ = topic;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    std::string errstr;
    
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("acks", "all", errstr);
    conf->set("retries", "3", errstr);
    conf->set("enable.idempotence", "false", errstr);
    conf->set("compression.type", "snappy", errstr);
    conf->set("broker.address.family", "v4", errstr);

    conf->set("batch.size", "16384", errstr);
    conf->set("linger.ms", "5", errstr);

    delivery_cb_.producer_ = this;
    conf->set("dr_cb", &delivery_cb_, errstr);

    producer_.reset(RdKafka::Producer::create(conf, errstr));

    if(!producer_)
    {
        std::cerr << "< F > Failed to create producer: " << errstr << std::endl;
        delete conf;
        delete tconf;
        return false;
    }

    topic_.reset(RdKafka::Topic::create(producer_.get(), topic, tconf, errstr));
    
    delete conf;
    delete tconf;

    std::cout << "< OK > Kafka producer initialized for " << brokers << " topic " << topic << std::endl;
    return true;
}

bool LibrdKafkaProducer::sendMessage(const std::string& key, const std::string& value)
{
    std::cout << "Sending message" << std::endl;
    if(!producer_)
    {
        std::cerr << "Продюсер RdKafka не проинициализирован" << std::endl;
        return false;
    }

    RdKafka::ErrorCode resp = producer_->produce(
        topic_name_,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(value.data()),
        value.size(),
        key.empty() ? nullptr : key.data(),
        key.size(),
        0,
        nullptr
    );

    if(resp != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Ошибка Продюсера RdKafka" << RdKafka::err2str(resp) << std::endl;
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.errors++;
        return false;
    }
    
    producer_->poll(0);
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.messages_sent++;
        stats_.bytes_sent += value.size();
    }
    std::cout << "Message sent" << std::endl;
    return true;
}

void LibrdKafkaProducer::DeliveryCb::dr_cb(RdKafka::Message& message)
{
    // if(!producer_ || producer_->stopping_)
    // {
    //     std::cerr << "Продюсер  RdKafka не существует или остановлен" << std::endl;
    //     return;
    // }
    if(message.err())
    {
        std::cerr << "Ошибка отправки сообщения: " << message.errstr() << " Topic: " << message.topic_name() << std::endl;

        if(producer_)
        {
            std::lock_guard<std::mutex> lock(producer_->stats_mutex_);
            producer_->stats_.errors++;
        }
    } else
        {
            std::cout << "Сообщение доставлено " << message.topic_name() << " " << message.partition() << " " << message.offset() << std::endl;
        }
}

ProducerStats LibrdKafkaProducer::getStats() const
{
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

bool LibrdKafkaProducer::flush(int timeout_ms)
{
    if(!producer_)
    {
        return false;
    }
    
    std::cout << "Принудительная отправка сообщения" << std::endl;
    return producer_->flush(timeout_ms);
}

bool LibrdKafkaProducer::sendJson(const Json::Value& j, const std::string& key)
{
    std::cout << "Sending json" << std::endl;
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    std::string json_str = Json::writeString(builder, j);
    std::cout << "Json sent" << std::endl;
    return sendMessage(key, json_str);
}