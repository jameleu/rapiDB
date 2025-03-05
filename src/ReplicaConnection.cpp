#include "ReplicaConnection.hpp"

ReplicaConnection::ReplicaConnection(int port, std::string replicaOfHost, int replicaOfPort)
    : listeningPort(port), offset(0), serverSocket(-1), stop(false), 
      masterPort(0), masterLink(false), masterLastIoTime(0) {
    runId = generateRunId();

    // run client listening thread AND master comm thread
    serverThread = std::thread(&ReplicaConnection::serverLoop, this);
    if (!replicaOfHost.empty() && replicaOfPort > 0) {
        std::cout << "Initializing replica of " << replicaOfHost << ":" << replicaOfPort << std::endl;
        
        masterHost = replicaOfHost;
        masterPort = replicaOfPort;
        
        masterConnectionThread = std::thread(&ReplicaConnection::connectToMaster, this);
    }
}

ReplicaConnection::~ReplicaConnection() {
    stop = true;
    
    if (serverSocket != -1) {
        close(serverSocket);
    }
    
    if (serverThread.joinable()) {
        serverThread.join();
    }
    
    if (masterConnectionThread.joinable()) {
        masterConnectionThread.join();
    }
    
    {
        std::lock_guard<std::mutex> lock(clientThreadsMutex);
        for (auto& thread : clientThreads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        clientThreads.clear();
    }
}

std::string ReplicaConnection::formatRespString(const std::string& str) {
    return "+" + str + "\r\n";
}

std::string ReplicaConnection::formatRespError(const std::string& str) {
    return "-" + str + "\r\n";
}

std::string ReplicaConnection::formatRespBulkString(const std::string& str) {
    return "$" + std::to_string(str.length()) + "\r\n" + str + "\r\n";
}

std::string ReplicaConnection::formatRESP(const std::vector<std::string>& args) {
    std::string resp = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
        resp += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return resp;
}

std::string ReplicaConnection::toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

// creates a separate thread for each client
void ReplicaConnection::serverLoop() {
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        std::cerr << "Error creating server socket\n";
        return;
    }

    int opt = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Error setting socket options\n";
        close(serverSocket);
        return;
    }

    struct sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(listeningPort);
    memset(serverAddr.sin_zero, 0, sizeof(serverAddr.sin_zero));

    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "Error binding server socket to port " << listeningPort << "\n";
        close(serverSocket);
        return;
    }

    if (listen(serverSocket, 5) < 0) {
        std::cerr << "Error listening on port " << listeningPort << "\n";
        close(serverSocket);
        return;
    }

    std::cout << "Replica listening on port " << listeningPort << std::endl;

    while (!stop) {
        struct sockaddr_in clientAddr;
        socklen_t addrSize = sizeof(clientAddr);
        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &addrSize);
        
        if (clientSocket < 0) {
            if (stop) break;
            std::cerr << "Error accepting connection\n";
            continue;
        }
        
        // create new thread to handle this client connection
        std::thread clientThread(&ReplicaConnection::handleClientConnection, this, clientSocket);
        
        {
            std::lock_guard<std::mutex> lock(clientThreadsMutex);
            
            // Clean up finished client threads
            for (auto it = clientThreads.begin(); it != clientThreads.end();) {
                if (!it->joinable()) {
                    it = clientThreads.erase(it);
                } else {
                    ++it;
                }
            }
            
            // Add the new client thread
            clientThreads.push_back(std::move(clientThread));
        }
    }

    close(serverSocket);
    serverSocket = -1;
}

void ReplicaConnection::handleClientConnection(int clientSocket) {
    // Get client IP and port for identifying if it's the master
    struct sockaddr_in clientAddr;
    socklen_t addrLen = sizeof(clientAddr);
    getpeername(clientSocket, (struct sockaddr*)&clientAddr, &addrLen);
    
    char clientIP[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(clientAddr.sin_addr), clientIP, INET_ADDRSTRLEN);
    int clientPort = ntohs(clientAddr.sin_port);
    
    // Check if this connection is from our configured master
    bool isFromMaster = isMasterConnection(clientIP, clientPort);
    
    // Buffer for accumulating data from potentially multiple recv calls
    std::string commandBuffer;
    char tempBuffer[1024];
    bool commandComplete = false;
    
    while (!stop) {
        commandBuffer.clear();
        commandComplete = false;
        
        // Receive complete RESP command
        while (!commandComplete && !stop) {
            memset(tempBuffer, 0, sizeof(tempBuffer));
            ssize_t bytesReceived = recv(clientSocket, tempBuffer, sizeof(tempBuffer) - 1, 0);
            
            if (bytesReceived <= 0) {
                goto cleanup;  
            }
            
            commandBuffer.append(tempBuffer, bytesReceived);
            
            try {
                RESPParser parser;
                RESPElement parsedCommand = parser.parse(commandBuffer);
                commandComplete = true; 
            } catch (const std::exception& e) {
                std::string error = e.what();
                if (error.find("Incomplete") != std::string::npos) {
                    // Need more data, continue receiving
                    continue;
                } else {
                    std::cerr << "Error parsing command: " << error << std::endl;
                    std::string errorResponse = "-ERR invalid command format\r\n";
                    send(clientSocket, errorResponse.c_str(), errorResponse.length(), 0);
                    commandComplete = true;  
                }
            }
        }
        
        if (!stop && !commandBuffer.empty()) {
            processCommand(commandBuffer, clientSocket, isFromMaster);
        }
    }

cleanup:
    close(clientSocket);
}

// Process command from client or master (just in case master connects here)
void ReplicaConnection::processCommand(std::string& commandBuffer, int clientSocket, bool isFromMaster) {
    try {
        RESPParser parser;
        RESPElement parsedCommand = parser.parse(commandBuffer);
        
        if (parsedCommand.type == RESPType::Array && !parsedCommand.array.empty()) {
            std::string command = parsedCommand.array[0].value;
            std::transform(command.begin(), command.end(), command.begin(), ::toupper);
            
            std::cout << "Received command " << command << " from " 
                      << (isFromMaster ? "master" : "client") << std::endl;
            
            if (isFromMaster) {
                processCommandFromMaster(commandBuffer);
                std::string okResponse = "+OK\r\n";
                send(clientSocket, okResponse.c_str(), okResponse.length(), 0);
            }
            else {   // Client commands
                if (command == "REPLCONF" || command == "PSYNC" || 
                    command == "INFO" || command == "WAIT") {
                    handleReplicationCommand(clientSocket, parsedCommand.array);
                }
                else if (command == "GET") {
                    handler.handleGet(clientSocket, parsedCommand.array);
                }
                else if (command == "EXISTS") {
                    handler.handleExists(clientSocket, parsedCommand.array);
                }
                else if (command == "PING") {
                    std::string pongResponse = "+PONG\r\n";
                    send(clientSocket, pongResponse.c_str(), pongResponse.length(), 0);
                }  
                // Reject writes on replica
                else if (command == "SET" || command == "DEL" || command == "INCR" ||
                         command == "DECR" || command == "LPUSH" || command == "RPUSH") {
                    std::string errorResponse = "-ERR READONLY You can't write against a read only replica.\r\n";
                    send(clientSocket, errorResponse.c_str(), errorResponse.length(), 0);
                }
                else {
                    std::string errorResponse = "-ERR unknown command '" + command + "'\r\n";
                    send(clientSocket, errorResponse.c_str(), errorResponse.length(), 0);
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error processing command: " << e.what() << std::endl;
        std::string errorResponse = "-ERR internal error\r\n";
        send(clientSocket, errorResponse.c_str(), errorResponse.length(), 0);
    }
}

void ReplicaConnection::setMaster(const std::string& host, int port) {
    masterHost = host;
    masterPort = port;
    masterLink = false;
    
    if (masterConnectionThread.joinable()) {
        masterConnectionThread.join();
    }
    
    masterConnectionThread = std::thread(&ReplicaConnection::connectToMaster, this);
}

// New method to handle master connection in a separate thread
void ReplicaConnection::connectToMaster() {
    if (masterHost.empty() || masterPort == 0) {
        std::cerr << "Master host or port not set\n";
        return;
    }
    
    std::cout << "Connecting to master at " << masterHost << ":" << masterPort << std::endl;
    
    sendPSyncToMaster();
}

void ReplicaConnection::processCommandFromMaster(const std::string& cmd) {
    // Update replication status
    offset += cmd.length();
    masterLastIoTime = time(NULL);
    
    // Parse RESP formatted command
    RESPParser parser;
    try {
        RESPElement parsed = parser.parse(cmd);
        
        if (parsed.type == RESPType::Array && !parsed.array.empty()) {
            std::string command = parsed.array[0].value;
            std::transform(command.begin(), command.end(), command.begin(), ::toupper);
            
            std::cout << "Replica executing: " << command;
            if (parsed.array.size() > 1) {
                std::cout << " " << parsed.array[1].value;
            }
            std::cout << std::endl;
            
            if (command == "REPLCONF" || command == "PSYNC" || command == "PING" || command == "WAIT") {
                handleReplicationCommand(-1, parsed.array);
                return;
            }
            
            // for internal operations
            int internalFd = -1;
            
            if (command == "SET") {
                handler.handleSet(internalFd, parsed.array);
            } 
            else if (command == "DEL") {
                handler.handleDel(internalFd, parsed.array);
            } 
            else if (command == "INCR") {
                handler.handleIncr(internalFd, parsed.array);
            } 
            else if (command == "DECR") {
                handler.handleDecr(internalFd, parsed.array);
            } 
            else if (command == "LPUSH") {
                handler.handleLPush(internalFd, parsed.array);
            } 
            else if (command == "RPUSH") {
                handler.handleRPush(internalFd, parsed.array);
            }
            else {
                std::cerr << "Replica: Unhandled command from master: " << command << std::endl;
            }
        }
    } 
    catch (const std::exception& e) {
        std::cerr << "Error processing command from master: " << e.what() << std::endl;
    }
}

void ReplicaConnection::handleReplicationCommand(int fd, const std::vector<RESPElement>& args) {
    if (args.empty()) return;
    
    std::string command = args[0].value;
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);
    
    if (command == "REPLCONF") {
        handleReplConf(fd, args);
    } 
    else if (command == "PSYNC") {
        handlePSync(fd, args);
    } 
    else if (command == "INFO") {
        handleInfo(fd, args);
    } 
    else if (command == "WAIT") {
        handleWait(fd, args);
    }
}

void ReplicaConnection::handleReplConf(int fd, const std::vector<RESPElement>& args) {
    if (args.size() < 2) {
        std::string response = formatRespError("ERR wrong number of arguments for 'REPLCONF' command");
        send(fd, response.c_str(), response.length(), 0);
        return;
    }
    
    std::string subCommand = args[1].value;
    std::transform(subCommand.begin(), subCommand.end(), subCommand.begin(), ::toupper);
    
    if (subCommand == "LISTENING-PORT" && args.size() >= 3) {
        // Master telling its listening port or client configuring master port
        int port = std::stoi(args[2].value);
        std::cout << "Received REPLCONF LISTENING-PORT " << port << std::endl;
        
        std::string response = formatRespString("OK");
        send(fd, response.c_str(), response.length(), 0);
    }
    else if (subCommand == "CAPA" && args.size() >= 3) {
        // Capability negotiation
        std::cout << "Received REPLCONF CAPA";
        for (size_t i = 2; i < args.size(); i++) {
            std::cout << " " << args[i].value;
        }
        std::cout << std::endl;
        
        std::string response = formatRespString("OK");
        send(fd, response.c_str(), response.length(), 0);
    }
    else if (subCommand == "ACK" && args.size() >= 3) {
        // Replication offset acknowledgment
        long long receivedOffset = std::stoll(args[2].value);
        std::cout << "Received REPLCONF ACK " << receivedOffset << std::endl;
        
        std::string response = formatRespString("OK");
        send(fd, response.c_str(), response.length(), 0);
    }
    else if (subCommand == "GETACK" && args.size() >= 2) {
        // Request for an ACK - respond with our current offset
        std::cout << "Received REPLCONF GETACK" << std::endl;
        
        std::string ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                                std::to_string(std::to_string(offset).length()) + 
                                "\r\n" + std::to_string(offset) + "\r\n";
        send(fd, ackResponse.c_str(), ackResponse.length(), 0);
    }
    else if ((subCommand == "MASTER-ID" || subCommand == "MASTER-RUNID") && args.size() >= 3) {
        // Master identifying itself to replica
        std::string masterId = args[2].value;
        std::cout << "Received REPLCONF " << subCommand << " " << masterId << std::endl;
        
        if (subCommand == "MASTER-RUNID") {
            replicationId = masterId;
        }
        
        std::string response = formatRespString("OK");
        send(fd, response.c_str(), response.length(), 0);
    }
    else {
        std::string response = formatRespError("ERR unknown REPLCONF subcommand or wrong number of arguments");
        send(fd, response.c_str(), response.length(), 0);
    }
}

void ReplicaConnection::handlePSync(int fd, const std::vector<RESPElement>& args) {
    if (args.size() < 3) {
        std::string response = formatRespError("ERR wrong number of arguments for 'PSYNC' command");
        send(fd, response.c_str(), response.length(), 0);
        return;
    }
    
    std::string requestedReplicationId = args[1].value;
    long long requestedOffset = std::stoll(args[2].value);
    
    std::cout << "Replica received PSYNC " << requestedReplicationId << " " << requestedOffset << std::endl;
    
    // respond appropriately to inform the sender
    if (fd >= 0 && isMasterConnection(getClientIP(fd).c_str(), getClientPort(fd))) {
        // Format: +REPLCONF listening-port <port> capa eof capa psync2
        std::string response = formatRESP({"REPLCONF", "listening-port", 
                                          std::to_string(listeningPort),
                                          "capa", "eof", "capa", "psync2"});
        send(fd, response.c_str(), response.length(), 0);
        
        // Send our replication status
        std::string replicationStatus = formatRESP({"REPLCONF", "ACK", 
                                                  std::to_string(offset)});
        send(fd, replicationStatus.c_str(), replicationStatus.length(), 0);
    }
    else if (fd >= 0) {  // client denied psync
        std::string response = formatRespError("ERR Can't PSYNC with a replica. If you want to subscribe to this replica's replication stream, use the SUBSCRIBE command.");
        send(fd, response.c_str(), response.length(), 0);
    }
}

void ReplicaConnection::handleInfo(int fd, const std::vector<RESPElement>& args) {
    std::string section = "all";
    if (args.size() > 1) {
        section = args[1].value;
        std::transform(section.begin(), section.end(), section.begin(), ::tolower);
    }
    
    std::string info;
    
    if (section == "replication" || section == "all") {
        info += "# Replication\r\n";
        info += "role:slave\r\n";
        info += "master_host:" + (masterHost.empty() ? "none" : masterHost) + "\r\n";
        info += "master_port:" + (masterPort ? std::to_string(masterPort) : "0") + "\r\n";
        info += "master_link_status:" + std::string(masterLink ? "up" : "down") + "\r\n";
        info += "master_last_io_seconds_ago:" + std::to_string(time(NULL) - masterLastIoTime) + "\r\n";
        info += "master_sync_in_progress:0\r\n";
        info += "slave_repl_offset:" + std::to_string(offset) + "\r\n";
        info += "slave_priority:100\r\n";
        info += "slave_read_only:1\r\n";
        info += "connected_slaves:0\r\n";
        info += "master_replid:" + replicationId + "\r\n";
        info += "master_replid2:0000000000000000000000000000000000000000\r\n";
        info += "master_repl_offset:" + std::to_string(offset) + "\r\n";
        info += "second_repl_offset:-1\r\n";
        info += "repl_backlog_active:1\r\n";
        info += "repl_backlog_size:1048576\r\n";
        info += "repl_backlog_first_byte_offset:0\r\n";
        info += "repl_backlog_histlen:" + std::to_string(offset) + "\r\n";
    }
    
    std::string response = formatRespBulkString(info);
    send(fd, response.c_str(), response.length(), 0);
}

// just respond to wait if caught up
void ReplicaConnection::handleWait(int fd, const std::vector<RESPElement>& args) {
    if (args.size() < 3) {
        std::string response = formatRespError("ERR wrong number of arguments for 'WAIT' command");
        send(fd, response.c_str(), response.length(), 0);
        return;
    }
    
    std::string response = ":0\r\n";  // Integer reply in RESP
    send(fd, response.c_str(), response.length(), 0);
}

void ReplicaConnection::sendPSyncToMaster() {
    int masterSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (masterSocket < 0) {
        std::cerr << "Error creating socket to connect to master\n";
        return;
    }
    
    struct sockaddr_in masterAddr;
    masterAddr.sin_family = AF_INET;
    masterAddr.sin_port = htons(masterPort);
    if (inet_pton(AF_INET, masterHost.c_str(), &masterAddr.sin_addr) <= 0) {
        std::cerr << "Invalid master address: " << masterHost << std::endl;
        close(masterSocket);
        return;
    }
    
    if (connect(masterSocket, (struct sockaddr*)&masterAddr, sizeof(masterAddr)) < 0) {
        std::cerr << "Error connecting to master at " << masterHost << ":" << masterPort << std::endl;
        close(masterSocket);
        return;
    }
    
    std::cout << "Connected to master at " << masterHost << ":" << masterPort << std::endl;
    
    // send a PING to check connection
    std::string pingCmd = formatRESP({"PING"});
    if (send(masterSocket, pingCmd.c_str(), pingCmd.length(), 0) < 0) {
        std::cerr << "Error sending PING to master\n";
        close(masterSocket);
        return;
    }
    
    // PONG response
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    if (recv(masterSocket, buffer, sizeof(buffer) - 1, 0) <= 0) {
        std::cerr << "Error receiving PONG from master\n";
        close(masterSocket);
        return;
    }
    
    // Send capabilities
    std::string capaCmd = formatRESP({"REPLCONF", "listening-port", 
                                     std::to_string(listeningPort)});
    if (send(masterSocket, capaCmd.c_str(), capaCmd.length(), 0) < 0) {
        std::cerr << "Error sending capabilities to master\n";
        close(masterSocket);
        return;
    }
    
    // Receive OK response
    memset(buffer, 0, sizeof(buffer));
    if (recv(masterSocket, buffer, sizeof(buffer) - 1, 0) <= 0) {
        std::cerr << "Error receiving capability acknowledgment from master\n";
        close(masterSocket);
        return;
    }
    
    // Send more capabilities
    std::string moreCapaCmd = formatRESP({"REPLCONF", "capa", "eof", "capa", "psync2"});
    if (send(masterSocket, moreCapaCmd.c_str(), moreCapaCmd.length(), 0) < 0) {
        std::cerr << "Error sending extended capabilities to master\n";
        close(masterSocket);
        return;
    }
    
    // Receive OK response
    memset(buffer, 0, sizeof(buffer));
    if (recv(masterSocket, buffer, sizeof(buffer) - 1, 0) <= 0) {
        std::cerr << "Error receiving extended capability acknowledgment from master\n";
        close(masterSocket);
        return;
    }
    
    std::string psyncCmd;
    if (replicationId.empty()) {
        // First sync: use "?" to request a full sync
        psyncCmd = formatRESP({"PSYNC", "?", "0"});
    } else {
        // Try partial sync with last known position
        psyncCmd = formatRESP({"PSYNC", replicationId, std::to_string(offset)});
    }
    
    if (send(masterSocket, psyncCmd.c_str(), psyncCmd.length(), 0) < 0) {
        std::cerr << "Error sending PSYNC to master\n";
        close(masterSocket);
        return;
    }
    
    processPSyncResponse(masterSocket);
}

void ReplicaConnection::processPSyncResponse(int masterSocket) {
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    
    ssize_t bytesRead = recv(masterSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead <= 0) {
        std::cerr << "Error receiving PSYNC response from master\n";
        close(masterSocket);
        return;
    }
    
    std::string response(buffer, bytesRead);
    
    // Response is either +FULLRESYNC <replid> <offset> or +CONTINUE <replid>
    if (response.substr(0, 11) == "+FULLRESYNC") {
        // Extract the replication ID and offset
        size_t firstSpace = response.find(' ', 1);
        size_t secondSpace = response.find(' ', firstSpace + 1);
        size_t lineEnd = response.find("\r\n");
        
        if (firstSpace != std::string::npos && secondSpace != std::string::npos && 
            lineEnd != std::string::npos) {
            replicationId = response.substr(firstSpace + 1, secondSpace - firstSpace - 1);
            offset = std::stoll(response.substr(secondSpace + 1, lineEnd - secondSpace - 1));
            
            std::cout << "Full resync with master: ID=" << replicationId 
                    << ", Offset=" << offset << std::endl;
            
            // Receive and load the RDB file
            receiveRDBFromMaster(masterSocket);
        } else {
            std::cerr << "Invalid FULLRESYNC response format\n";
        }
    } 
    else if (response.substr(0, 9) == "+CONTINUE") {
        size_t firstSpace = response.find(' ', 1);
        size_t lineEnd = response.find("\r\n");
        
        if (firstSpace != std::string::npos && lineEnd != std::string::npos) {
            replicationId = response.substr(firstSpace + 1, lineEnd - firstSpace - 1);
            
            std::cout << "Partial resync with master: ID=" << replicationId 
                    << ", Offset=" << offset << std::endl;
            
        } else {
            std::cerr << "Invalid CONTINUE response format\n";
        }
    } 
    else {
        std::cerr << "Unexpected PSYNC response: " << response << std::endl;
        close(masterSocket);
        return;
    }
    
    masterLink = true;
    masterLastIoTime = time(NULL);
    
    processMasterStream(masterSocket);
}

void ReplicaConnection::receiveRDBFromMaster(int masterSocket) {
    std::cout << "Receiving RDB file from master..." << std::endl;
    
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    
    ssize_t bytesRead = recv(masterSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead <= 0) {
        std::cerr << "Error receiving RDB length from master\n";
        close(masterSocket);
        return;
    }
    
    std::string lengthStr(buffer, bytesRead);
    
    if (lengthStr[0] != '$') {
        std::cerr << "Expected bulk string marker, got: " << lengthStr << std::endl;
        close(masterSocket);
        return;
    }
    
    size_t crlfPos = lengthStr.find("\r\n");
    if (crlfPos == std::string::npos) {
        std::cerr << "Invalid RESP format for RDB length\n";
        close(masterSocket);
        return;
    }
    
    size_t rdbLength = std::stoll(lengthStr.substr(1, crlfPos - 1));
    std::cout << "RDB file size: " << rdbLength << " bytes" << std::endl;
    
    std::string rdbData;
    rdbData.reserve(rdbLength);
    
    // Skip any data already read that's part of the RDB file
    if (crlfPos + 2 < bytesRead) {
        rdbData.append(lengthStr.substr(crlfPos + 2));
    }
    
    // Read the rest of the RDB file
    while (rdbData.size() < rdbLength) {
        memset(buffer, 0, sizeof(buffer));
        bytesRead = recv(masterSocket, buffer, std::min(sizeof(buffer) - 1, 
                                                       rdbLength - rdbData.size()), 0);
        if (bytesRead <= 0) {
            std::cerr << "Error receiving RDB content from master\n";
            close(masterSocket);
            return;
        }
        
        rdbData.append(buffer, bytesRead);
        std::cout << "Received " << rdbData.size() << " of " << rdbLength 
                << " bytes (" << (rdbData.size() * 100 / rdbLength) << "%)\r" << std::flush;
    }
    
    std::cout << std::endl << "RDB file received completely." << std::endl;
    
    // Load the RDB file into our database
    loadRDBData(rdbData);
}

// Load RDB data into database
void ReplicaConnection::loadRDBData(const std::string& rdbData) {
    std::cout << "Loading RDB data into database..." << std::endl;
    
    // Save the RDB data to a temporary file
    std::string tempFilename = "/tmp/redis_rdb_" + std::to_string(getpid()) + ".rdb";
    
    try {
        std::ofstream outFile(tempFilename, std::ios::binary);
        if (!outFile) {
            throw std::runtime_error("Failed to create temporary file: " + tempFilename);
        }
        
        outFile.write(rdbData.c_str(), rdbData.size());
        outFile.close();
        
        // Use the database's own load method
        if (!handler.db->loadRDB(tempFilename)) {
            throw std::runtime_error("Failed to load RDB data into database");
        }
        
        // Delete the temporary file
        std::remove(tempFilename.c_str());
        
        std::cout << "RDB data loaded successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error loading RDB data: " << e.what() << std::endl;
        std::remove(tempFilename.c_str());
    }
}

void ReplicaConnection::processMasterStream(int masterSocket) {
    std::cout << "Processing command stream from master..." << std::endl;
    
    // Buffer for accumulating RESP commands
    std::string buffer;
    char tempBuffer[1024];
    
    while (!stop) {
        memset(tempBuffer, 0, sizeof(tempBuffer));
        ssize_t bytesRead = recv(masterSocket, tempBuffer, sizeof(tempBuffer) - 1, 0);
        
        if (bytesRead <= 0) {
            std::cerr << "Master connection closed or error\n";
            masterLink = false;
            close(masterSocket);
            
            break;
        }
        
        masterLastIoTime = time(NULL);
        buffer.append(tempBuffer, bytesRead);
        
        // Process complete RESP commands
        while (!buffer.empty()) {
            try {
                RESPParser parser;
                RESPElement parsedCommand = parser.parse(buffer);
                
                // Successfully parsed a complete command
                std::string command = parsedCommand.array[0].value;
                buffer.clear();
                
                processCommandFromMaster(command);
                
            } catch (const std::exception& e) {
                std::string error = e.what();
                if (error.find("Incomplete") != std::string::npos) {
                    // Need more data, wait for next recv
                    break;
                } else {
                    // Parsing error, discard the command
                    std::cerr << "Error parsing command from master: " << error << std::endl;
                    buffer.clear();
                    break;
                }
            }
        }
    }
}

// Master connection status check
bool ReplicaConnection::isMasterConnection(const char* clientIP, int clientPort) {
    if (masterHost.empty() || masterPort == 0) {
        return false;
    }
    
    // Check if the IP matches configured master's IP
    if (strcmp(clientIP, masterHost.c_str()) == 0) {
        return true;
    }
    
    // Resolve hostname if needed
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    
    if (getaddrinfo(masterHost.c_str(), nullptr, &hints, &res) == 0) {
        for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
            struct sockaddr_in* addr = (struct sockaddr_in*)p->ai_addr;
            char resolvedIP[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &(addr->sin_addr), resolvedIP, INET_ADDRSTRLEN);
            
            if (strcmp(clientIP, resolvedIP) == 0) {
                freeaddrinfo(res);
                return true;
            }
        }
        freeaddrinfo(res);
    }
    return false;
}

std::string ReplicaConnection::getClientIP(int clientSocket) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    
    if (getpeername(clientSocket, (struct sockaddr*)&addr, &addrLen) == 0) {
        char ipStr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(addr.sin_addr), ipStr, INET_ADDRSTRLEN);
        return std::string(ipStr);
    }
    
    return "unknown";
}

int ReplicaConnection::getClientPort(int clientSocket) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    
    if (getpeername(clientSocket, (struct sockaddr*)&addr, &addrLen) == 0) {
        return ntohs(addr.sin_port);
    }
    
    return -1;
}

std::string ReplicaConnection::generateRunId() {
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

void ReplicaConnection::updateReplicationStatus(long long newOffset) {
    offset = newOffset;
    masterLastIoTime = time(NULL);
}