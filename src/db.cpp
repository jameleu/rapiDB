#include "DB.hpp"
#include <sstream>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>

DB& DB::getInstance() {
    static DB instance;  // singleton
    return instance;
}

// start up db -> load from rdb file
DB::DB() {
    loadRDB();
}

// shut down to db -> save to rdb file
DB::~DB() {
    saveRDB();
}

// write string to ofstream:    length stringS
void DB::writeString(std::ofstream &out, const std::string &s) {
    uint64_t length = s.size();
    out.write(reinterpret_cast<const char*>(&length), sizeof(length));
    out.write(s.data(), length);
}

// read string from ifstream
// get length (first space delimited item)
// then read that length of chars next for the string
std::string DB::readString(std::ifstream &in) {
    uint64_t length = 0;
    in.read(reinterpret_cast<char*>(&length), sizeof(length));
    std::string s(length, '\0');
    in.read(&s[0], length);
    return s;
}

// Save the database state to dump.rdb
void DB::saveRDB() {
    std::ofstream out("dump.rdb", std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Failed to open dump.rdb for saving." << std::endl;
        return;
    }
    
    // for strings
    {
        std::scoped_lock strLock(stringMutex_, expireMutex_);  // avoids deadlocks + nested locks
        uint64_t numStrings = stringStore_.size();
        out.write(reinterpret_cast<const char*>(&numStrings), sizeof(numStrings));
        for (const auto& pair : stringStore_) {
            // Write key and value using length-prefixed format.
            writeString(out, pair.first);
            writeString(out, pair.second);
            
            // Write expiration (if any). Use -1 to indicate no expiration.
            int64_t expiration = -1;
            if (expirationStore_.find(pair.first) != expirationStore_.end())
                expiration = expirationStore_[pair.first];
            out.write(reinterpret_cast<const char*>(&expiration), sizeof(expiration));
        }
    }
    
    // for lists
    {
        std::scoped_lock strLock(listMutex_, expireMutex_);  // avoids deadlocks + nested locks
        uint64_t numLists = listStore_.size();
        out.write(reinterpret_cast<const char*>(&numLists), sizeof(numLists));
        for (const auto& pair : listStore_) {
            // Write key using length-prefixed format.
            writeString(out, pair.first);
            
            // Write number of elements in the list.
            uint64_t numElements = pair.second.size();
            out.write(reinterpret_cast<const char*>(&numElements), sizeof(numElements));
            
            // Write each list element.
            for (const auto &element : pair.second) {
                writeString(out, element);
            }
            
            // Write expiration for this key; -1 means no expiration.
            int64_t expiration = -1;
           
            if (expirationStore_.find(pair.first) != expirationStore_.end())
                expiration = expirationStore_[pair.first];

            out.write(reinterpret_cast<const char*>(&expiration), sizeof(expiration));
        }
    }
    std::cout << "DB saved to dump.rdb" << std::endl;
}

// Load the database state to dump.rdb
void DB::loadRDB() {
    std::ifstream in("dump.rdb", std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "No RDB file found, starting with an empty DB." << std::endl;
        return;
    }
    
    // for strings
    {
        std::scoped_lock strLock(stringMutex_, expireMutex_);  // avoids deadlocks + nested locks
        uint64_t numStrings = 0;
        in.read(reinterpret_cast<char*>(&numStrings), sizeof(numStrings));
        for (uint64_t i = 0; i < numStrings; ++i) {
            std::string key = readString(in);  // read length, then length of that for key
            std::string value = readString(in);
            int64_t expiration;
            in.read(reinterpret_cast<char*>(&expiration), sizeof(expiration));
            
            stringStore_[key] = value;
            if (expiration != -1) {
                expirationStore_[key] = expiration;
            }
        }
    }
    // for lists
    {
        std::scoped_lock strLock(listMutex_, expireMutex_);  // avoids deadlocks + nested locks
        uint64_t numLists = 0;
        in.read(reinterpret_cast<char*>(&numLists), sizeof(numLists));
        for (uint64_t i = 0; i < numLists; ++i) {
            std::string key = readString(in);  // read length, then length of that for key
            
            // Read number of list elements.
            uint64_t numElements = 0;
            in.read(reinterpret_cast<char*>(&numElements), sizeof(numElements));
            
            std::vector<std::string> elements;
            for (uint64_t j = 0; j < numElements; ++j) {
                std::string element = readString(in);
                elements.push_back(element);
            }
            
            int64_t expiration;
            in.read(reinterpret_cast<char*>(&expiration), sizeof(expiration));
            
            listStore_[key] = elements;
            if (expiration != -1) {
                expirationStore_[key] = expiration;
            }
        }
    }
    std::cout << "DB loaded from dump.rdb" << std::endl;
}

// check if expired and erase if so, then throw error
void DB::throwDeleteIfExpired(const std::string& key) {
    if (isExpired(key)) {
        erase(key);
        throw std::runtime_error("Key has expired");
    }
}

// check Is Expired
bool DB::isExpired(const std::string& key) {
    std::lock_guard<std::mutex> lock(expireMutex_);
    auto it = expirationStore_.find(key);
    if (it == expirationStore_.end()) return false;
    auto now = std::chrono::system_clock::now();

    auto unixTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    return unixTimeMs > it->second;
}
void DB::setExpirationTime(const std::string& key, long expiry) {
    std::lock_guard<std::mutex> lock(expireMutex_);
    expirationStore_[key] = expiry;
}

// set expiration as infinite
void DB::setExpirationInf(const std::string& key) {
    std::lock_guard<std::mutex> lock(expireMutex_);
    expirationStore_.erase(key);
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