#include "Handler.hpp"
#include <stdexcept>
#include <string>
#include <iostream>
#include <cstdlib>
#include <sys/socket.h>

// Argument format: SET key [keys...]
// Sets value at key, overwriting if applicable. Resets TTL if applicable.
// Returns OK if successful
void Handler::handleSet(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 3) {
            throw std::runtime_error("Invalid SET command format");
        }

        // Extract key and value from the request array.
        std::string key = requestArray[1].value;
        std::string value = requestArray[2].value;

        // Store in the in-memory database.
        database[key] = value;

        std::string response = "+OK\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// Argument format: GET key [keys...]
// returns value at key. returns error otherwise if not string
void Handler::handleGet(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid GET command format");
        }

        std::string key = requestArray[1].value;

        auto it = database.find(key);
        if (it != database.end()) {
            // Key exists: return the value as a bulk string.
            std::string value = it->second;
            std::string response = "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
            send(fd, response.c_str(), response.length(), 0);
        } else {
            // Key does not exist: return a null bulk string.
            std::string response = "$-1\r\n";
            send(fd, response.c_str(), response.length(), 0);
        }
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// Argument format: EXISTS key [keys...]
// Shows if key exists
// Returns 1 if exists (sums up for each key given, even duplicates)
void Handler::handleExists(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid EXISTS command format");
        }

        std::string key;
        int num_found = 0;
        for (size_t i = 1; i < requestArray.size(); i++) {
            key = requestArray[1].value;
            if (database.find(key) != database.end())
            {
                num_found++;
            }
        }
        std::string response = ":" + std::to_string(num_found) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// Argument format: DEL key [keys...]
// Deletes key value pair. Ignores key if it does not exist
// Returns number of keys deleted
void Handler::handleDel(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid DEL command format");
        }
        int num_deleted = 0;
        std::string key;
        for (size_t i = 1; i < requestArray.size(); i++) {
            key = requestArray[i].value;
            database.erase(key);
            num_deleted++;
        }
        std::string response = ":" + std::to_string(num_deleted) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// Argument format: INCR key
// Increments value by one. Returns error if is not intenger
// Returns new value of key
void Handler::handleIncr(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid INCR command format");
        }

        std::string key = requestArray[1].value;
        auto it = database.find(key);

        if (it != database.end()) {
            try {
                int64_t value = std::stoll(it->second);
                value++; 
                it->second = std::to_string(value);
            } catch (const std::exception&) {
                std::string error_response = "-Error: Value is not an integer\r\n";
                send(fd, error_response.c_str(), error_response.length(), 0);
                return;
            }
        } else {
            database[key] = "1";
        }

        std::string response = ":" + database[key] + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// Argument format: DECR key
// Decrements value by one. Returns error if is not intenger
// Returns new value of key
void Handler::handleDecr(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid DECR command format");
        }

        std::string key = requestArray[1].value;
        auto it = database.find(key);

        if (it != database.end()) {
            try {
                int64_t value = std::stoll(it->second);
                value--; 
                it->second = std::to_string(value);
            } catch (const std::exception&) {
                std::string error_response = "-Error: Value is not an integer\r\n";
                send(fd, error_response.c_str(), error_response.length(), 0);
                return;
            }
        } else {
            database[key] = "-1";
        }

        std::string response = ":" + database[key] + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// LPUSH key value [value ...]
// Inserts values at the head (left side) of the list.
// Returns size of array after changes
void Handler::handleLPush(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() < 3) {
            throw std::runtime_error("Invalid LPUSH command format");
        }
        std::string key = requestArray[1].value;
        // In Redis, LPUSH inserts values one by one, so the final order is reversed relative to the command order.
        for (size_t i = 2; i < requestArray.size(); i++) {
            listDatabase[key].insert(listDatabase[key].begin(), requestArray[i].value);
        }
        size_t newLength = listDatabase[key].size();
        std::string response = ":" + std::to_string(newLength) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    } catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// RPUSH key value [value ...]
// Appends values to the tail (right side) of the list.
// Returns new length of array at key value
void Handler::handleRPush(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() < 3) {
            throw std::runtime_error("Invalid RPUSH command format");
        }
        std::string key = requestArray[1].value;
        for (size_t i = 2; i < requestArray.size(); i++) {  // start after key in command list
            listDatabase[key].push_back(requestArray[i].value);
        }
        size_t newLength = listDatabase[key].size();
        std::string response = ":" + std::to_string(newLength) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    } catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

// LRANGE key start stop
// Returns the list elements between indices start and stop (inclusive).
void Handler::handleLRange(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 4) {
            throw std::runtime_error("Invalid LRANGE command format");
        }
        std::string key = requestArray[1].value;
        int start = std::stoi(requestArray[2].value);
        int stop  = std::stoi(requestArray[3].value);

        auto it = listDatabase.find(key);
        std::vector<std::string> list;

        // get list if it exists
        if (it != listDatabase.end()) {
            list = it->second;
        }

        int listSize = list.size();
        // if negative, give it as IDX from end
        if (start < 0) start = listSize + start;
        // if negative, give it as IDX from end
        if (stop < 0) stop = listSize + stop;

        // Hard clamps in case list size is still not big enough for negatives
        if (start < 0) start = 0;
        // or if given end is out of bounds still
        if (stop >= listSize) stop = listSize - 1;

        // if list does not exist, return empty
        if (start > stop || listSize == 0) {
            std::string response = "*0\r\n";
            send(fd, response.c_str(), response.length(), 0);
            return;
        }

        int count = stop - start + 1;
        std::ostringstream oss;
        oss << "*" << count << "\r\n";
        for (int i = start; i <= stop; i++) {
            std::string element = list[i];
            oss << "$" << element.length() << "\r\n" << element << "\r\n";
        }
        std::string response = oss.str();
        send(fd, response.c_str(), response.length(), 0);

    } catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}
