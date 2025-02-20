#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include "resp_parser.hpp" 

class Handler {
public:
    Handler() = default;

    void handleSet(int fd, const std::vector<RESPElement>& requestArray);
    void handleGet(int fd, const std::vector<RESPElement>& requestArray);
    void handleExists(int fd, const std::vector<RESPElement>& requestArray);
    void handleDel(int fd, const std::vector<RESPElement>& requestArray);
    void handleIncr(int fd, const std::vector<RESPElement>& requestArray);
    void handleDecr(int fd, const std::vector<RESPElement>& requestArray);
    void handleLPush(int fd, const std::vector<RESPElement>& requestArray);
    void handleRPush(int fd, const std::vector<RESPElement>& requestArray);

private:
    std::unordered_map<std::string, std::string> database;
};

#endif // HANDLER_HPP
