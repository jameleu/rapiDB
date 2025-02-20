#include "DB.hpp"
#include <sstream>
#include <cstdlib>

DB& DB::getInstance() {
    static DB instance;  // singleton
    return instance;
}

void DB::set(const std::string& key, const std::string& value) {
    std::scoped_lock strLock(stringMutex_, listMutex_);  // avoids deadlocks

    // always overwrites
    listStore_.erase(key);

    stringStore_[key] = value;
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
    return "$-1\r\n";   // does not exist; send null string
}

bool DB::exist(const std::string& key) {
    // check if exists as string or list
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

bool DB::erase(const std::string& key) {
    bool deleted = false;
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        if (stringStore_.erase(key) > 0) {
            deleted = true;
        }
    }
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        if (listStore_.erase(key) > 0) {
            deleted = true;
        }
    }
    return deleted;
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
    // Check that the key is not in the string store with temporary lock.
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        if (stringStore_.find(key) != stringStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    // get key's value and append or create new list if it does not exist
    std::lock_guard<std::mutex> listLock(listMutex_);
    auto it = listStore_.find(key);
    if (it == listStore_.end()) {
        listStore_[key] = std::vector<std::string>{ value };
    } else {
        it->second.insert(it->second.begin(), value);
    }
}

size_t DB::sizeOf(const std::string& key) {
    // return size of string
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        auto it = stringStore_.find(key);
        if (it != stringStore_.end()) {
            return it->second.size();
        }
    }

    // return size of list
    {
        std::lock_guard<std::mutex> listLock(listMutex_);
        auto it = listStore_.find(key);
        if (it != listStore_.end()) {
            return it->second.size();
        }
    }
    return 0; // 0 if does not exist
}


void DB::rpush(const std::string& key, const std::string& value) {
    // Check that the key is not in the string store.
    {
        std::lock_guard<std::mutex> strLock(stringMutex_);
        if (stringStore_.find(key) != stringStore_.end()) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
    // get key's value and append or create new list if it does not exist
    std::lock_guard<std::mutex> listLock(listMutex_);
    auto it = listStore_.find(key);
    if (it == listStore_.end()) {
        listStore_[key] = std::vector<std::string>{ value };
    } else {
        it->second.push_back(value);
    }
}

std::vector<std::string> DB::lrange(const std::string& key, int start, int stop) {
    std::lock_guard<std::mutex> listLock(listMutex_);
    auto it = listStore_.find(key);
    if (it == listStore_.end()) {
        return {};
    }

    const auto& list = it->second;
    int size = static_cast<int>(list.size());

    // set negative values as idx relative from end, just like python does
    if (start < 0) {
        start = size + start;
        if (start < 0) start = 0; // clamping in case size is not big enough
    }

    // negative values are idx relative from end
    if (stop < 0) {
        stop = size + stop;
        if (stop < 0) stop = 0; // clamping so stop is not out of bounds
    }

    // end clamping relative to end
    if (stop >= size) stop = size - 1;
    if (start > stop) return {}; // invalid range

    return std::vector<std::string>(list.begin() + start, list.begin() + stop + 1);
}