#include <iostream>


#include "replication_message.h"
#include "pgoutput_parser.h"

#include <sstream>

#include <arpa/inet.h>

#include <string.h>

//##################################################### BEGIN MSG ######################################################## 
void pgoutput::BeginMessage::parse(const std::vector<uint8_t> &data,  PgOutputParser *parser)
{
    size_t offset = 0;
    // Int64       - final LSN (8 байт)
    // Int64       - commit timestamp (8 байт)  
    // Int32       - transaction XID (4 байта)

    // uint32_t lsn_high; // = *reinterpret_cast<const uint32_t*>(&data[offset]);
    // memcpy(&lsn_high, &data[offset], 4);
    // lsn_high = ntohl(lsn_high);
    // offset += 4;

    // uint32_t lsn_low; // = *reinterpret_cast<const uint32_t*>(&data[offset]);
    // memcpy(&lsn_low, &data[offset], 4);
    // lsn_low = ntohl(lsn_low);
    // offset += 4;

    lsn_ = parser->read_uint64(data, offset);

    // commit_timestamp_ = *reinterpret_cast<const uint64_t*>(&data[offset]);
    // offset +=8;
    commit_timestamp_ = parser->read_uint64(data, offset);

    transaction_id_ = *reinterpret_cast<const uint32_t*>(&data[offset]);
    std::cout << pgoutput::BeginMessage::toString();
}

std::string pgoutput::BeginMessage::toString() const
{
    std::stringstream ss;
    ss << "BEGIN [LSN: " << std::hex << lsn_ 
       << ", XID: " << std::dec << transaction_id_ 
       << ", Timestamp: " << commit_timestamp_ << "]\n";
    return ss.str();
}

//##################################################### RELATION MSG ######################################################## 
void pgoutput::RelationMessage::parse(const std::vector<uint8_t> &data,  PgOutputParser *parser)
{
    size_t offset = 0;
    // uint32_t net_oid;
    // memcpy(&net_oid, &data[offset], 4);
    // relation_id_ = ntohl(net_oid);
    // offset += 4;
    relation_id_ = parser->read_uint32(data, offset);

    // namespace_ = std::string(reinterpret_cast<const char*>(&data[offset]));
    // offset += namespace_.size() + 1;
    namespace_ = parser->read_string(data, offset);
    // relation_name_ = std::string(reinterpret_cast<const char*>(&data[offset]));
    // offset += relation_name_.size() + 1;
    relation_name_ = parser->read_string(data, offset);

    replica_identity_ = data[offset++];

    // uint16_t net_num;
    // memcpy(&net_num, &data[offset], 2);
    // uint16_t num_columns = ntohs(net_num);
    // offset += 2;

    uint16_t num_columns = parser->read_uint16(data, offset);

    columns_.resize(num_columns);
    for(int i = 0; i < num_columns; ++i)
    {
        // columns_[i].flags = *reinterpret_cast<const uint8_t*>(&data[offset++]);
        columns_[i].flags = parser->read_uint8(data, offset);
        

        // columns_[i].name = std::string(reinterpret_cast<const char*>(&data[offset]));
        // offset += columns_[i].name.size() + 1;
        columns_[i].name = parser->read_string(data, offset);

        // uint32_t type;
        // memcpy(&type, &data[offset], 4);
        // columns_[i].type_id = ntohl(type);
        // offset += 4;
        columns_[i].type_id = parser->read_uint32(data, offset);

        // uint32_t modifier;
        // memcpy(&modifier, &data[offset], 4);
        // columns_[i].type_modifier = ntohl(modifier);
        // offset +=4;
        columns_[i].type_modifier = parser->read_uint32(data, offset);
    }
    std::cout << pgoutput::RelationMessage::toString();
}

std::string pgoutput::RelationMessage::toString() const
{
    std::stringstream ss;
    ss << "RELATION [id: " << relation_id_ 
       << ", schema: " << namespace_ 
       << ", table: " << relation_name_ 
       << ", columns: " << columns_.size() << "]\n";
    
    for (size_t i = 0; i < columns_.size(); ++i) {
        ss << "  Column " << i << ": " << columns_[i].toString() << "\n";
    }
    
    return ss.str();
}

//##################################################### COMMIT MSG ######################################################## 

void pgoutput::CommitMessage::parse(const std::vector<uint8_t> &data,  PgOutputParser *parser)
{
    size_t offset = 0;

    flags_ = data[offset++];

    // uint32_t lsn_high; // = *reinterpret_cast<const uint32_t*>(&data[offset]);
    // memcpy(&lsn_high, &data[offset], 4);
    // lsn_high = ntohl(lsn_high);
    // offset += 4;

    // uint32_t lsn_low; // = *reinterpret_cast<const uint32_t*>(&data[offset]);
    // memcpy(&lsn_low, &data[offset], 4);
    // lsn_low = ntohl(lsn_low);
    // offset += 4;

    // lsn_ = (static_cast<const uint64_t>(lsn_high) << 32) | lsn_low;
    lsn_ = parser->read_uint64(data, offset);

    // uint32_t end_lsn_high = *reinterpret_cast<const uint32_t*>(&data[offset]);
    // end_lsn_high = ntohl(end_lsn_high);
    // offset += 4;
    // uint32_t end_lsn_low = *reinterpret_cast<const uint32_t*>(&data[offset]);
    // end_lsn_low = ntohl(end_lsn_low);
    // offset += 4;

    // end_lsn_ = (static_cast<uint64_t>(end_lsn_high) << 32) | end_lsn_low;
    end_lsn_ = parser->read_uint64(data, offset);

    // commit_timestamp_ = *reinterpret_cast<const uint64_t*>(&data[offset]);
    // offset += 8;
    commit_timestamp_ = parser->read_uint64(data, offset);

    std::cout << pgoutput::CommitMessage::toString();
}

std::string pgoutput::CommitMessage::toString() const {
    std::stringstream ss;
    ss << "COMMIT [flags: " << static_cast<int>(flags_) 
       << ", LSN: " << std::hex << lsn_ 
       << ", end LSN: " << end_lsn_ 
       << ", timestamp: " << std::dec << commit_timestamp_ << "]\n";
    return ss.str();
}


//##################################################### INSERT MSG ########################################################
void pgoutput::InsertMessage::parse(const std::vector<uint8_t>& data,  PgOutputParser *parser)
{
    size_t offset = 0;

    // uint32_t rel_id;
    // memcpy(&rel_id, &data[offset], 4);
    // relation_id_ = ntohl(rel_id);
    // offset += 4;
    relation_id_ = parser->read_uint32(data, offset);
    tuple_type_ = parser->read_uint8(data, offset);

    auto relation = parser->getCachedRelation(relation_id_);
       
    if(relation)
    {
       new_tuple_ = parser->parseTuple(data, offset, relation->getColumns()); // use cash , not this or this!!!!!! ADD METHOD
    } else
        {
            new_tuple_ = parser->parseTuple(data, offset); 
        }



    // new_tuple_ = parser->parseTuple(data, offset);

    std::cout << pgoutput::InsertMessage::toString();
}

std::string pgoutput::InsertMessage::toString() const
{
    std::stringstream ss;
    ss << "INSERT [relation_id: " << relation_id_ 
       << ", tuple_type: " << tuple_type_ 
       << ", values: ";
    
    for (size_t i = 0; i < new_tuple_.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << new_tuple_[i];
    }
    ss << "]\n";
    
    return ss.str();
}
//##################################################### DELETE MSG ########################################################

void pgoutput::DeleteMessage::parse(const std::vector<uint8_t>& data, PgOutputParser *parser)
{
    size_t offset = 0;

    // uint32_t rel_id;
    // memcpy(&rel_id, &data[offset], 4);
    // relation_id_ = ntohl(rel_id);
    // offset += 4;
    relation_id_ = parser->read_uint32(data, offset);

    uint8_t flags = parser->read_uint8(data, offset);

    auto relation = parser->getCachedRelation(relation_id_);

    if(relation)
    {
        old_tuple_ = parser->parseTuple(data, offset, relation->getColumns());
    } else 
        {
            old_tuple_ = parser->parseTuple(data, offset);
        }

    // old_tuple_ = parser->parseTuple(data, offset);
    std::cout << pgoutput::DeleteMessage::toString();
}

std::string pgoutput::DeleteMessage::toString() const
{
    std::stringstream ss;
    ss << "DELETE [relation_id: " << relation_id_;
    
    for (size_t i = 0; i < old_tuple_.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << old_tuple_[i];
    }
    ss << "]\n";

    return ss.str();
}

//##################################################### UPDATE MSG ########################################################

void pgoutput::UpdateMessage::parse(const std::vector<uint8_t>& data, PgOutputParser *parser)
{
    size_t offset = 0;

    // uint32_t rel_id;
    // memcpy(&rel_id, &data[offset], 4);
    // relation_id_ = ntohl(rel_id);
    // offset += 4;
    relation_id_ = parser->read_uint32(data, offset);

    uint8_t flags = parser->read_uint8(data, offset);

    uint8_t has_old_tuple = data[offset];

    auto relation = parser->getCachedRelation(relation_id_);

    if(relation)
    {
        if(has_old_tuple == 'O')
        {
            old_tuple_ = parser->parseTuple(data, offset,relation->getColumns());
        }
        new_tuple_ = parser->parseTuple(data, offset,relation->getColumns());
    } else
        {
            if(has_old_tuple == 'O')
                {
                    std::cout << "READ PARSE TUPLE OLD UPDATE" << std::endl;
                    old_tuple_ = parser->parseTuple(data, offset);
                }
                std::cout << "READ PARSE TUPLE NEW UPDATE" << std::endl;
            new_tuple_ = parser->parseTuple(data, offset);
        }

    // new_tuple_ = parser->parseTuple(data, offset);
    std::cout << pgoutput::UpdateMessage::toString();
}

std::string pgoutput::UpdateMessage::toString() const
{
    std::stringstream ss;
    ss << "UPDATE [relation_id: " << relation_id_;
    
    if (!old_tuple_.empty()) {
        ss << ", old_values: ";
        for (size_t i = 0; i < old_tuple_.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << old_tuple_[i];
        }
    }
    
    ss << ", new_values: ";
    for (size_t i = 0; i < new_tuple_.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << new_tuple_[i];
    }
    ss << "]\n";
    
    return ss.str();
}

//##################################################### TRUNCATE MSG ########################################################

void pgoutput::TruncateMessage::parse(const std::vector<uint8_t>& data, PgOutputParser *parser)
{
    size_t offset;

    // uint32_t xid;
    // memcpy(&xid, &data[offset], 4);
    // transaction_id_ = ntohl(xid);
    // offset +=4;
    transaction_id_ = parser->read_uint32(data, offset);

    // uint32_t num_of_rel;
    // memcpy(&num_of_rel, &data[offset], 4);
    // number_of_relations_ = ntohl(num_of_rel);
    // offset += 4;
    number_of_relations_ = parser->read_uint32(data, offset);

    options_ = *reinterpret_cast<const uint8_t*>(&data[offset++]);
    // 0x00 = 0b00000000 - нет опций
    // 0x01 = 0b00000001 - только CASCADE  
    // 0x02 = 0b00000010 - только RESTART IDENTITY
    // 0x03 = 0b00000011 - и CASCADE, и RESTART IDENTITY

    relation_ids_.clear();
    relation_ids_.reserve(number_of_relations_);

    for(uint32_t i = 0; i < number_of_relations_; ++i)
    {
        u_int32_t oid = *reinterpret_cast<const uint32_t*>(&data[offset]);
        relation_ids_.push_back(oid);
    }

     std::cout << "TRUNCATE parsed: " << number_of_relations_ 
              << " tables, options=0x" << std::hex << (int)options_ 
              << std::dec << std::endl;
}

std::string pgoutput::TruncateMessage::toString() const
{
    std::stringstream ss;
    
    ss << "TRUNCATE [";
    ss << "transaction_id_ = " << transaction_id_ << ", ";
    if (options_ != 0) {
        ss << "options: ";
        if (options_ & 0x01) ss << "CASCADE";
        if (options_ & 0x02) {
            if (options_ & 0x01) ss << "|";
            ss << "RESTART IDENTITY";
        }
        ss << ", ";
    }
    
    ss << "tables: ";
    for (size_t i = 0; i < relation_ids_.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << relation_ids_[i];
    }
    
    ss << "]\n";
    
    return ss.str();
}