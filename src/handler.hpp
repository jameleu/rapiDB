#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include "resp_parser.hpp" 
#include "db.hpp"

class Handler {
    // wrapper functions for db that do resp conversion/handling, calling on parser for input
    // + formatting output
public:
    Handler();
    void handleSet(int fd, const std::vector<RESPElement>& requestArray);
    void handleGet(int fd, const std::vector<RESPElement>& requestArray);
    void handleExists(int fd, const std::vector<RESPElement>& requestArray);
    void handleDel(int fd, const std::vector<RESPElement>& requestArray);
    void handleIncr(int fd, const std::vector<RESPElement>& requestArray);
    void handleDecr(int fd, const std::vector<RESPElement>& requestArray);
    void handleLPush(int fd, const std::vector<RESPElement>& requestArray);
    void handleRPush(int fd, const std::vector<RESPElement>& requestArray);
    void handleLRange(int fd, const std::vector<RESPElement>& requestArray);

private:
    DB* db;  // singleton db
    void sendErrorMessage(int fd, const std::string& errorMessage);
};

#endif // HANDLER_HPP
