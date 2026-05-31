#pragma once


#include <map>
#include <memory>
#include <iostream>
#include <sstream>

#include "replication_message.h"
#include "../shared_structures.h"

namespace pgoutput {

class PgOutputParser
{
    public:
        uint8_t read_uint8(const std::vector<uint8_t>& data, size_t& offset);
        uint16_t read_uint16(const std::vector<uint8_t>& data, size_t& offset);
        uint32_t read_uint32(const std::vector<uint8_t>& data, size_t& offset);
        uint64_t read_uint64(const std::vector<uint8_t>& data, size_t& offset);
        std::string read_string(const std::vector<uint8_t>& data, size_t& offset);
        int64_t read_int64(const std::vector<uint8_t>& data, size_t& offset);

        std::shared_ptr<ReplicationMessage> parseMessage(const std::vector<uint8_t> raw_message);
        std::vector<std::string> parseTuple(const std::vector<uint8_t>& data, size_t& offset);
        std::vector<std::string> parseTuple(const std::vector<uint8_t>& data, size_t& offset, const std::vector<pgoutput::ReplicationMessage::Column> &columns);
        void cacheRelation(const std::shared_ptr<RelationMessage>& relation);
        std::shared_ptr<pgoutput::RelationMessage> getCachedRelation(uint32_t relation_id);
        void getRelationInfo();

        // size_t getRelationSize(const std::vector<uint8_t>& msg);
    private:
        std::map<uint32_t, std::shared_ptr<RelationMessage>> relation_cache_;
};

}