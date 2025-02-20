#include "Handler.hpp"
#include <stdexcept>
#include <string>
#include <iostream>
#include <cstdlib>
#include <sys/socket.h>
#include <sstream> 
Handler::Handler()
{
    DB& db = DB::getInstance();
}

void Handler::sendErrorMessage(int fd, const std::string& errorMessage) {
    std::string redisError = "-ERR " + errorMessage + "\r\n";   // -ERR is resp
    send(fd, redisError.c_str(), redisError.length(), 0);
}

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

        db->set(key, value);

        std::string response = "+OK\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
    }
}

// Argument format: GET key
// returns value at key. returns error otherwise if not string
void Handler::handleGet(int fd, const std::vector<RESPElement>& requestArray) {
    try {
        if (requestArray.size() != 2) {
            throw std::runtime_error("Invalid GET command format");
        }

        std::string key = requestArray[1].value;
        std::string value = db->get(key);
        std::string response = "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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
            key = requestArray[i].value;
            if (db->exist(key))
            {
                num_found++;
            }
        }
        std::string response = ":" + std::to_string(num_found) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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
            if (db->erase(key))
            {
                num_deleted++;
            }
        }
        std::string response = ":" + std::to_string(num_deleted) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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
        int new_val = db->incr(key);        

        std::string response = ":" + std::to_string(new_val) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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
        int new_val = db->incr(key);        

        std::string response = ":" + std::to_string(new_val) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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
            db->lpush(key, requestArray[i].value);
        }
        size_t newLength = db->sizeOf(key);
        std::string response = ":" + std::to_string(newLength) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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
            db->rpush(key, requestArray[i].value);
        }
        size_t newLength = db->sizeOf(key);
        std::string response = ":" + std::to_string(newLength) + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
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

        std::vector<std::string> snippet = db->lrange(key, start, stop);

        //resp formatting
        std::ostringstream response;
        response << "*" << (stop - start + 1) << "\r\n";
        for (int i = 0; i < snippet.size(); ++i) {
            const std::string& value = snippet[i];
            response << "$" << value.size() << "\r\n" << value << "\r\n"; // Bulk string format
        }
        std::string responseStr = response.str();
        send(fd, responseStr.c_str(), responseStr.length(), 0);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        sendErrorMessage(fd, e.what());
    }
}
