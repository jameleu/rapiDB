#ifndef REPLICA_CONNECTION_HPP
#define REPLICA_CONNECTION_HPP

#include <string>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sstream>
#include <random>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "DB.hpp"
#include "resp_parser.hpp"
#include "handler.hpp"

class ReplicaConnection {
private:
    int listeningPort;
    std::atomic<long long> offset;
    int serverSocket;
    std::atomic<bool> stop;
    std::thread serverThread;
    std::thread masterConnectionThread;
    std::mutex clientThreadsMutex;
    std::vector<std::thread> clientThreads;
    Handler handler;

    // Replication state
    std::string masterHost;
    int masterPort;
    std::string replicationId;     // Master's replication ID
    std::string runId;             // Unique ID for this replica
    std::atomic<bool> masterLink;  // If connected to master
    std::atomic<long long> masterLastIoTime;  // Last interaction time with master
    
    // RESP formatting helpers
    std::string formatRespString(const std::string& str);
    std::string formatRespError(const std::string& str);
    std::string formatRespBulkString(const std::string& str);
    std::string formatRESP(const std::vector<std::string>& args);
    std::string toUpper(const std::string& str);

    // Network helpers
    bool isMasterConnection(const char* clientIP, int clientPort);
    std::string getClientIP(int clientSocket);
    int getClientPort(int clientSocket);
    
    // Command processing
    void processCommand(std::string& commandBuffer, int clientSocket, bool isFromMaster);
    void processCommandFromMaster(const std::string& cmd);
    
    // Replication protocol handlers
    void handleReplicationCommand(int fd, const std::vector<RESPElement>& args);
    void handleReplConf(int fd, const std::vector<RESPElement>& args);
    void handlePSync(int fd, const std::vector<RESPElement>& args);
    void handleInfo(int fd, const std::vector<RESPElement>& args);
    void handleWait(int fd, const std::vector<RESPElement>& args);
    
    // Server and client handling
    void serverLoop();
    void handleClientConnection(int clientSocket);
    
    // Master connection methods
    void connectToMaster();
    void processPSyncResponse(int masterSocket);
    void receiveRDBFromMaster(int masterSocket);
    void loadRDBData(const std::string& rdbData);
    void processMasterStream(int masterSocket);
    
    // Utility methods
    std::string generateRunId();

public:
    // Constructor and destructor
    ReplicaConnection(int port, std::string replicaOfHost, int replicaOfPort);
    ~ReplicaConnection();
    
    // Public interface methods
    void setMaster(const std::string& host, int port);
    void updateReplicationStatus(long long newOffset);
    void sendPSyncToMaster();
    
    // Get replica status
    bool isMasterConnected() const { return masterLink; }
    long long getOffset() const { return offset; }
    std::string getReplicationId() const { return replicationId; }
    std::string getRunId() const { return runId; }
};

#endif // REPLICA_CONNECTION_HPP