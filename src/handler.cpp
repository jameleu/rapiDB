#include "Handler.hpp"
#include <stdexcept>
#include <string>
#include <iostream>
#include <cstdlib>
#include <sys/socket.h>

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

void Handler::handleExists(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid EXISTS command format");
        }

        std::string key = requestArray[1].value;
        bool exists = (database.find(key) != database.end());
        std::string response = ":" + std::to_string(exists ? 1 : 0) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

void Handler::handleDel(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid DEL command format");
        }

        std::string key = requestArray[1].value;
        size_t num_deleted = database.erase(key);
        std::string response = ":" + std::to_string(num_deleted) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::string error_response = "-Error: " + std::string(e.what()) + "\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
    }
}

void Handler::handleIncr(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid INCR command format");
        }

        std::string key = requestArray[1].value;
        auto it = database.find(key);

        if (it != database.end()) {
            int64_t value = std::stoll(it->second);
            value++;
            it->second = std::to_string(value);
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

void Handler::handleDecr(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid DECR command format");
        }

        std::string key = requestArray[1].value;
        auto it = database.find(key);

        if (it != database.end()) {
            int64_t value = std::stoll(it->second);
            value--;
            it->second = std::to_string(value);
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
