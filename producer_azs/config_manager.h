#pragma once

#include <string>
#include <map>
#include <cstdint>


class ConfigManager
{
    public:
        ConfigManager(const std::string &filename);
        ConfigManager(){}
        ~ConfigManager();

        void load();
        void save(const std::string &key, const std::string &value);
        std::string getValueByKeyFromConfig(const std::string &key) const;
        void parseLSN(const std::string& lsn_str, uint32_t& high, uint32_t& low);

    private:
        std::string filename_;
        std::map<std::string, std::string> values_;
};