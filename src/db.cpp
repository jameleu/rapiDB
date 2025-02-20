#include "DB.hpp"
#include <sstream>
#include <cstdlib>

DB& DB::getInstance() {
    static DB instance;
    return instance;
}

void DB::set(const std::string& key, const std::string& value) {
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        if (listStore_.find(key) != listStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        stringStore_[key] = value;
    }
}

std::string DB::get(const std::string& key) {
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        auto it = stringStore_.find(key);
        if (it != stringStore_.end()) {
            return it->second;
        }
    }
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        if (listStore_.find(key) != listStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    throw std::runtime_error("Key does not exist");
}

bool DB::exist(const std::string& key) {
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        if (stringStore_.find(key) != stringStore_.end())
            return true;
    }
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        if (listStore_.find(key) != listStore_.end())
            return true;
    }
    return false;
}

void DB::erase(const std::string& key) {
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        stringStore_.erase(key);
    }
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        listStore_.erase(key);
    }
}

int DB::incr(const std::string& key) {
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        if (listStore_.find(key) != listStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    std::lock_guard<std::mutex> strLock(stringMutex_);
    auto it = stringStore_.find(key);
    if (it == stringStore_.end()) {
        stringStore_[key] = "1";
        return 1;
    }
    int num = 0;
    try {
        num = std::stoi(it->second);
    } catch (...) {
        throw std::runtime_error("Value is not an integer");
    }
    num++;
    it->second = std::to_string(num);
    return num;
}

int DB::decr(const std::string& key) {
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        if (listStore_.find(key) != listStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    std::lock_guard<std::mutex> strLock(stringMutex_);
    auto it = stringStore_.find(key);
    if (it == stringStore_.end()) {
        stringStore_[key] = "-1";
        return -1;
    }
    int num = 0;
    try {
        num = std::stoi(it->second);
    } catch (...) {
        throw std::runtime_error("Value is not an integer");
    }
    num--;
    it->second = std::to_string(num);
    return num;
}

void DB::lpush(const std::string& key, const std::string& value) {
    // Check that the key is not in the string store.
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        if (stringStore_.find(key) != stringStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    std::lock_guard<std::mutex> listLock(listMutex_);
    auto it = listStore_.find(key);
    if (it == listStore_.end()) {
        listStore_[key] = std::vector<std::string>{ value };
    } else {
        it->second.insert(it->second.begin(), value);
    }
}

void DB::rpush(const std::string& key, const std::string& value) {
    // Check that the key is not in the string store.
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        if (stringStore_.find(key) != stringStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    std::lock_guard<std::mutex> listLock(listMutex_);
    auto it = listStore_.find(key);
    if (it == listStore_.end()) {
        listStore_[key] = std::vector<std::string>{ value };
    } else {
        it->second.push_back(value);
    }
}

std::vector<std::string> DB::lrange(const std::string& key, int start, int stop) {
    // Does not take negative/unclamped start and stop (fix that before using this; quicker db that way)
    std::lock_guard<std::mutex> listLock(listMutex_);
    auto it = listStore_.find(key);
    if (it == listStore_.end()) {
        return {};
    }
    const auto& list = it->second;
    int size = static_cast<int>(list.size());

    return std::vector<std::string>(list.begin() + start, list.begin() + stop + 1);
}
