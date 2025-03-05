#ifndef MASTER_SERVER_HPP
#define MASTER_SERVER_HPP

#include <string>
#include <vector>
#include <mutex>
#include <random>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "DB.hpp"
#include "ReplicaConnection.hpp"

class MasterServer {
private:
    struct ReplicaInfo {
        int socket;
        struct sockaddr_in addr;
        int port;
        std::string host;
        bool connected;
        long long offset;
        
        ReplicaInfo(const std::string& h, int p) : 
            socket(-1), port(p), host(h), connected(false), offset(0) {
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = inet_addr(host.c_str());
        }
    };
    
    std::vector<ReplicaInfo> replicas;
    std::mutex mutex;
    int masterPort;
    std::string masterId;
    std::string masterRunId;
    DB& db;
    long long replicationOffset;

    std::string generateMasterRunId();
    
    std::string formatRESP(const std::vector<std::string> &args);
    
    bool performReplicationHandshake(ReplicaInfo& replica);
    
    bool connectToReplica(ReplicaInfo& replica);
    
    void connectToAllReplicas();
    
    std::string generateRDBSnapshot();
    
    void handlePSYNC(int clientSocket, const std::vector<RESPElement>& args);

public:
    MasterServer(int port);
    ~MasterServer();
    
    void addReplica(const std::string& host, int port);
    void addReplica(int port);
    
    void removeReplica(const std::string& host, int port);
    
    bool sendCommand(const std::vector<std::string> &cmdArgs);
    
    bool propagateWrite(const std::vector<std::string> &cmdArgs);
    
    void handleReplicationCommand(int clientSocket, const std::vector<RESPElement>& args);
    
    std::string getMasterInfo() const;
    
    int getConnectedReplicaCount() const;
    
    std::vector<std::pair<std::string, int>> getReplicaList() const;
    
    std::string getRunId() const { return masterRunId; }
    
    long long getOffset() const { return replicationOffset; }
};

#endif // MASTER_SERVER_HPP