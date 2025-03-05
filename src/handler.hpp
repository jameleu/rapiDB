#ifndef HANDLER_HPP
#define HANDLER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "resp_parser.hpp" 
#include "db.hpp"

class Handler {
public:
    DB* db;  // singleton
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

    std::string infoReplication();
    std::string toUpper(const std::string& str);

private:

    void sendErrorMessage(int fd, const std::string& errorMessage);

    bool isReplica;
    int replicaListeningPort;
    size_t replicaOffset;
    int masterOffset;
    std::string replicationID;
    std::mutex replicaMutex;
};

#endif // HANDLER_HPP
