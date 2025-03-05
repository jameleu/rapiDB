#include "MasterServer.hpp"
#include <sstream>
#include <fstream>
#include <algorithm>

MasterServer::MasterServer(int port) 
    : masterPort(port), db(DB::getInstance()), replicationOffset(0) {
    masterRunId = generateMasterRunId();
    masterId = "master_" + std::to_string(getpid());
}

MasterServer::~MasterServer() {
    for (auto& replica : replicas) {
        if (replica.socket >= 0) {
            close(replica.socket);
            replica.socket = -1;
        }
    }
}

std::string MasterServer::generateMasterRunId() {
    const char *charset = "0123456789abcdef";
    std::string id;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 15);
    
    for (int i = 0; i < 40; i++) {
        id += charset[dist(gen)];
    }
    
    return id;
}

std::string MasterServer::formatRESP(const std::vector<std::string> &args) {
    std::string resp = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto &arg : args) {
        resp += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return resp;
}

bool MasterServer::performReplicationHandshake(ReplicaInfo& replica) {
    // 1. Send PING to check connection
    std::string pingCmd = formatRESP({"PING"});
    if (send(replica.socket, pingCmd.c_str(), pingCmd.size(), 0) < 0) {
        std::cerr << "Error sending PING during handshake to " << replica.host << ":" << replica.port << "\n";
        return false;
    }
    
    // Wait for PONG response
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    ssize_t bytesRead = recv(replica.socket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead <= 0) {
        std::cerr << "Error receiving PONG response\n";
        return false;
    }
    
    // 2. Send REPLCONF to identify as master
    std::string replconfListeningPort = formatRESP({"REPLCONF", "listening-port", std::to_string(masterPort)});
    if (send(replica.socket, replconfListeningPort.c_str(), replconfListeningPort.size(), 0) < 0) {
        std::cerr << "Error sending REPLCONF listening-port to " << replica.host << ":" << replica.port << "\n";
        return false;
    }
    
    // Wait for OK response
    memset(buffer, 0, sizeof(buffer));
    bytesRead = recv(replica.socket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead <= 0) {
        std::cerr << "Error receiving OK for REPLCONF listening-port\n";
        return false;
    }
    
    // 3. Send REPLCONF capa (capabilities)
    std::string replconfCapa = formatRESP({"REPLCONF", "capa", "eof", "capa", "psync2"});
    if (send(replica.socket, replconfCapa.c_str(), replconfCapa.size(), 0) < 0) {
        std::cerr << "Error sending REPLCONF capa to " << replica.host << ":" << replica.port << "\n";
        return false;
    }
    
    // Wait for OK response
    memset(buffer, 0, sizeof(buffer));
    bytesRead = recv(replica.socket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead <= 0) {
        std::cerr << "Error receiving OK for REPLCONF capa\n";
        return false;
    }
    
    // 4. Send master info for identification
    std::string infoCmd = formatRESP({"REPLCONF", "master-id", masterId, "master-runid", masterRunId});
    if (send(replica.socket, infoCmd.c_str(), infoCmd.size(), 0) < 0) {
        std::cerr << "Error sending master info to " << replica.host << ":" << replica.port << "\n";
        return false;
    }
    
    // Wait for OK response
    memset(buffer, 0, sizeof(buffer));
    bytesRead = recv(replica.socket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead <= 0) {
        std::cerr << "Error receiving OK for master info\n";
        return false;
    }
    
    std::cout << "Master handshake completed with replica on " << replica.host << ":" << replica.port << "\n";
    return true;
}

bool MasterServer::connectToReplica(ReplicaInfo& replica) {
    if (replica.connected && replica.socket >= 0) {
        return true; // Already connected
    }
    
    replica.socket = socket(AF_INET, SOCK_STREAM, 0);
    if (replica.socket < 0) {
        std::cerr << "Error creating master socket for replica " << replica.host << ":" << replica.port << "\n";
        return false;
    }
    
    // Set socket options for keeping connection alive
    int flag = 1;
    if (setsockopt(replica.socket, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag)) < 0) {
        std::cerr << "Error setting socket options for replica " << replica.host << ":" << replica.port << "\n";
        close(replica.socket);
        replica.socket = -1;
        return false;
    }
    
    if (connect(replica.socket, (struct sockaddr*)&replica.addr, sizeof(replica.addr)) < 0) {
        std::cerr << "Error connecting to replica on " << replica.host << ":" << replica.port << "\n";
        close(replica.socket);
        replica.socket = -1;
        return false;
    }
    
    // Perform RESP handshake to identify as master
    if (!performReplicationHandshake(replica)) {
        close(replica.socket);
        replica.socket = -1;
        return false;
    }
    
    replica.connected = true;
    replica.offset = replicationOffset; 
    
    return true;
}

void MasterServer::connectToAllReplicas() {
    for (auto& replica : replicas) {
        connectToReplica(replica);
    }
}

void MasterServer::addReplica(const std::string& host, int port) {
    std::lock_guard<std::mutex> lock(mutex);
    
    for (const auto& replica : replicas) {
        if (replica.host == host && replica.port == port) {
            std::cerr << "Replica " << host << ":" << port << " already in the list\n";
            return;
        }
    }
    
    replicas.emplace_back(host, port);
    
    connectToReplica(replicas.back());
    
    std::cout << "Added replica " << host << ":" << port << " to master\n";
}

void MasterServer::addReplica(int port) {
    addReplica("127.0.0.1", port);
}

void MasterServer::removeReplica(const std::string& host, int port) {
    std::lock_guard<std::mutex> lock(mutex);
    
    for (auto it = replicas.begin(); it != replicas.end(); ++it) {
        if (it->host == host && it->port == port) {
            if (it->socket >= 0) {
                close(it->socket);
            }
            replicas.erase(it);
            std::cout << "Removed replica " << host << ":" << port << "\n";
            return;
        }
    }
    
    std::cerr << "Replica " << host << ":" << port << " not found in the list\n";
}

bool MasterServer::sendCommand(const std::vector<std::string> &cmdArgs) {
    std::lock_guard<std::mutex> lock(mutex);
    
    if (replicas.empty()) {
        std::cerr << "No replicas configured. Command not sent.\n";
        return false;
    }
    
    bool allSucceeded = true;
    std::string formattedCmd = formatRESP(cmdArgs);
    
    replicationOffset += formattedCmd.size();
    
    for (auto& replica : replicas) {
        if (!replica.connected || replica.socket < 0) {
            if (!connectToReplica(replica)) {
                allSucceeded = false;
                continue;
            }
        }
        
        ssize_t sentBytes = send(replica.socket, formattedCmd.c_str(), formattedCmd.size(), 0);
        if (sentBytes < 0) {
            std::cerr << "Error sending command to " << replica.host << ":" << replica.port << ", attempting to reconnect\n";
            close(replica.socket);
            replica.socket = -1;
            replica.connected = false;
            
            if (!connectToReplica(replica)) {
                allSucceeded = false;
                continue;
            }
            
            sentBytes = send(replica.socket, formattedCmd.c_str(), formattedCmd.size(), 0);
            if (sentBytes < 0) {
                std::cerr << "Failed to send command after reconnection to " << replica.host << ":" << replica.port << "\n";
                close(replica.socket);
                replica.socket = -1;
                replica.connected = false;
                allSucceeded = false;
            }
        }
        
        if (sentBytes > 0) {
            replica.offset += sentBytes;
        }
    }
    
    return allSucceeded;
}

bool MasterServer::propagateWrite(const std::vector<std::string> &cmdArgs) {
    return sendCommand(cmdArgs);
}

void MasterServer::handleReplicationCommand(int clientSocket, const std::vector<RESPElement>& args) {
    if (args.empty()) {
        std::string response = "-ERR invalid replication command\r\n";
        send(clientSocket, response.c_str(), response.length(), 0);
        return;
    }
    
    std::string command = args[0].value;
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);
    
    if (command == "PSYNC") {
        handlePSYNC(clientSocket, args);
    }
    else if (command == "REPLCONF") {
        if (args.size() >= 2) {
            std::string subCommand = args[1].value;
            std::transform(subCommand.begin(), subCommand.end(), subCommand.begin(), ::toupper);
            
            if (subCommand == "ACK" && args.size() >= 3) {
                std::string response = "+OK\r\n";
                send(clientSocket, response.c_str(), response.length(), 0);
            }
            else {
                std::string response = "+OK\r\n";
                send(clientSocket, response.c_str(), response.length(), 0);
            }
        }
        else {
            std::string response = "-ERR wrong number of arguments for 'REPLCONF' command\r\n";
            send(clientSocket, response.c_str(), response.length(), 0);
        }
    }
    else if (command == "INFO") {
        // Generate INFO replication section
        std::string info = "# Replication\r\n";
        info += "role:master\r\n";
        info += "master_replid:" + masterRunId + "\r\n";
        info += "master_replid2:0000000000000000000000000000000000000000\r\n";
        info += "master_repl_offset:" + std::to_string(replicationOffset) + "\r\n";
        info += "second_repl_offset:-1\r\n";
        info += "repl_backlog_active:1\r\n";
        info += "repl_backlog_size:1048576\r\n";
        info += "repl_backlog_first_byte_offset:0\r\n";
        info += "repl_backlog_histlen:" + std::to_string(replicationOffset) + "\r\n";
        info += "connected_slaves:" + std::to_string(getConnectedReplicaCount()) + "\r\n";
        
        // Add info for each connected replica
        int slaveIndex = 0;
        for (const auto& replica : replicas) {
            if (replica.connected) {
                info += "slave" + std::to_string(slaveIndex) + ":ip=" + replica.host + 
                        ",port=" + std::to_string(replica.port) + 
                        ",state=online,offset=" + std::to_string(replica.offset) + 
                        ",lag=0\r\n";
                slaveIndex++;
            }
        }
        
        // Send the info as a RESP bulk string
        std::string response = "$" + std::to_string(info.length()) + "\r\n" + info + "\r\n";
        send(clientSocket, response.c_str(), response.length(), 0);
    }
    else if (command == "WAIT") {
        if (args.size() >= 3) {
            int numReplicas = std::stoi(args[1].value);
            int timeout = std::stoi(args[2].value);
            
            // Get number of connected replicas that have caught up
            int ackedReplicas = 0;
            {
                std::lock_guard<std::mutex> lock(mutex);
                for (const auto& replica : replicas) {
                    if (replica.connected && replica.offset >= replicationOffset) {
                        ackedReplicas++;
                    }
                }
            }
            
            // If we don't have enough replicas, wait up to timeout
            if (ackedReplicas < numReplicas) {
                usleep(std::min(timeout, 100) * 1000);
                
                // Recount after waiting
                ackedReplicas = 0;
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    for (const auto& replica : replicas) {
                        if (replica.connected && replica.offset >= replicationOffset) {
                            ackedReplicas++;
                        }
                    }
                }
            }
            
            std::string response = ":" + std::to_string(ackedReplicas) + "\r\n";
            send(clientSocket, response.c_str(), response.length(), 0);
        }
        else {
            std::string response = "-ERR wrong number of arguments for 'WAIT' command\r\n";
            send(clientSocket, response.c_str(), response.length(), 0);
        }
    }
    else {
        std::string response = "-ERR unknown replication command\r\n";
        send(clientSocket, response.c_str(), response.length(), 0);
    }
}

std::string MasterServer::generateRDBSnapshot() {
    std::string rdbData = "REDIS0009";  // Redis RDB version 9
    
    rdbData += "\xFF";
    
    return rdbData;
}

void MasterServer::handlePSYNC(int clientSocket, const std::vector<RESPElement>& args) {
    if (args.size() < 3) {
        std::string response = "-ERR wrong number of arguments for 'PSYNC' command\r\n";
        send(clientSocket, response.c_str(), response.length(), 0);
        return;
    }
    
    std::string requestedReplicationId = args[1].value;
    std::string requestedOffsetStr = args[2].value;
    long long requestedOffset = 0;
    
    try {
        requestedOffset = std::stoll(requestedOffsetStr);
    } catch (const std::exception& e) {
        std::string response = "-ERR invalid PSYNC offset\r\n";
        send(clientSocket, response.c_str(), response.length(), 0);
        return;
    }
    
    std::cout << "Received PSYNC " << requestedReplicationId << " " << requestedOffset << std::endl;
    
    // Handle PSYNC request
    // If the replica is asking for initial sync (? as replication ID)
    // or if the replication ID doesn't match ours, do a full resync
    if (requestedReplicationId == "?" || requestedReplicationId != masterRunId) {
        // Full resync
        std::string fullResyncResponse = "+FULLRESYNC " + masterRunId + " " + std::to_string(replicationOffset) + "\r\n";
        if (send(clientSocket, fullResyncResponse.c_str(), fullResyncResponse.length(), 0) < 0) {
            std::cerr << "Error sending FULLRESYNC response\n";
            return;
        }
        
        std::cout << "Sending FULLRESYNC response: " << fullResyncResponse;
        
        // Generate RDB snapshot
        std::string rdbSnapshot = generateRDBSnapshot();
        
        // Send RDB snapshot as a RESP bulk string
        std::string rdbHeader = "$" + std::to_string(rdbSnapshot.size()) + "\r\n";
        if (send(clientSocket, rdbHeader.c_str(), rdbHeader.length(), 0) < 0) {
            std::cerr << "Error sending RDB header\n";
            return;
        }
        
        if (send(clientSocket, rdbSnapshot.c_str(), rdbSnapshot.size(), 0) < 0) {
            std::cerr << "Error sending RDB data\n";
            return;
        }
        
        std::string crlf = "\r\n";
        if (send(clientSocket, crlf.c_str(), crlf.length(), 0) < 0) {
            std::cerr << "Error sending CRLF after RDB data\n";
            return;
        }
        
        std::cout << "Sent RDB snapshot, size: " << rdbSnapshot.size() << " bytes\n";
        
        struct sockaddr_in addr;
        socklen_t addrLen = sizeof(addr);
        getpeername(clientSocket, (struct sockaddr*)&addr, &addrLen);
        
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(addr.sin_addr), ip, INET_ADDRSTRLEN);
        int port = ntohs(addr.sin_port);
        
        std::string host(ip);
        
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            for (auto& replica : replicas) {
                if (replica.host == host && replica.port == port) {
                    // Update existing replica
                    replica.socket = clientSocket;
                    replica.connected = true;
                    replica.offset = replicationOffset;
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                // Add new replica
                replicas.emplace_back(host, port);
                replicas.back().socket = clientSocket;
                replicas.back().connected = true;
                replicas.back().offset = replicationOffset;
                
                std::cout << "Added new replica: " << host << ":" << port << "\n";
            }
        }
    }
    else if (requestedOffset <= replicationOffset) {
        // Partial resync
        std::string continueResponse = "+CONTINUE " + masterRunId + "\r\n";
        if (send(clientSocket, continueResponse.c_str(), continueResponse.length(), 0) < 0) {
            std::cerr << "Error sending CONTINUE response\n";
            return;
        }
        
        std::cout << "Sending CONTINUE response for partial resync from offset " << requestedOffset << "\n";
        
        // Update replica information
        struct sockaddr_in addr;
        socklen_t addrLen = sizeof(addr);
        getpeername(clientSocket, (struct sockaddr*)&addr, &addrLen);
        
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(addr.sin_addr), ip, INET_ADDRSTRLEN);
        int port = ntohs(addr.sin_port);
        
        std::string host(ip);
        
        // Update replica offset
        {
            std::lock_guard<std::mutex> lock(mutex);
            for (auto& replica : replicas) {
                if (replica.host == host && replica.port == port) {
                    replica.socket = clientSocket;
                    replica.connected = true;
                    replica.offset = requestedOffset;
                    break;
                }
            }
        }
    }
    else {
        // Full resync
        std::string fullResyncResponse = "+FULLRESYNC " + masterRunId + " " + std::to_string(replicationOffset) + "\r\n";
        if (send(clientSocket, fullResyncResponse.c_str(), fullResyncResponse.length(), 0) < 0) {
            std::cerr << "Error sending FULLRESYNC response\n";
            return;
        }
        
        std::cout << "Sending FULLRESYNC response (offset in future): " << fullResyncResponse;
        
        // Send RDB snapshot as above
        std::string rdbSnapshot = generateRDBSnapshot();
        std::string rdbHeader = "$" + std::to_string(rdbSnapshot.size()) + "\r\n";
        
        if (send(clientSocket, rdbHeader.c_str(), rdbHeader.length(), 0) < 0) {
            std::cerr << "Error sending RDB header\n";
            return;
        }
        
        if (send(clientSocket, rdbSnapshot.c_str(), rdbSnapshot.size(), 0) < 0) {
            std::cerr << "Error sending RDB data\n";
            return;
        }
        
        // Additional CRLF after RDB data
        std::string crlf = "\r\n";
        if (send(clientSocket, crlf.c_str(), crlf.length(), 0) < 0) {
            std::cerr << "Error sending CRLF after RDB data\n";
            return;
        }
        
        std::cout << "Sent RDB snapshot, size: " << rdbSnapshot.size() << " bytes\n";
        
    }
}

std::string MasterServer::getMasterInfo() const {
    return "id:" + masterId + ",runid:" + masterRunId + ",port:" + std::to_string(masterPort) + 
           ",replicas:" + std::to_string(replicas.size());
}

int MasterServer::getConnectedReplicaCount() const {
    int count = 0;
    for (const auto& replica : replicas) {
        if (replica.connected) {
            count++;
        }
    }
    return count;
}

std::vector<std::pair<std::string, int>> MasterServer::getReplicaList() const {
    std::vector<std::pair<std::string, int>> result;
    for (const auto& replica : replicas) {
        result.emplace_back(replica.host, replica.port);
    }
    return result;
}