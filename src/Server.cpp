#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <chrono>
#include <random>
#include <arpa/inet.h> 
#include <netinet/in.h>
#include <sys/socket.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "resp_parser.hpp"
#include "Handler.hpp"
#include "MasterServer.hpp"

#define BUFFER_SIZE 128

void processRequest(int fd, const RESPElement& requestArr, Handler & handler, MasterServer * master) {
    std::vector<RESPElement> requestArray = requestArr.array;
    if (requestArray.empty()) return;

    std::string command = requestArray[0].value;
    
    // Extract command arguments for replication
    std::vector<std::string> cmdArgs;
    for (const auto& elem : requestArray) {
        cmdArgs.push_back(elem.value);
    }
    
    // Process different Redis commands with appropriate replication
    if (command == "SET") {
        handler.handleSet(fd, requestArray);
        // Propagate write commands to replicas if we're a master
        master->propagateWrite(cmdArgs);
    }
    else if (command == "GET") {  // no propagation for reads
        handler.handleGet(fd, requestArray);
    }
    else if (command == "EXISTS") {
        handler.handleExists(fd, requestArray);
    }
    else if (command == "DEL") {
        handler.handleDel(fd, requestArray);
        master->propagateWrite(cmdArgs);
    }
    else if (command == "INCR") {
        handler.handleIncr(fd, requestArray);
        master->propagateWrite(cmdArgs);
    }
    else if (command == "DECR") {
        handler.handleDecr(fd, requestArray);
        master->propagateWrite(cmdArgs);
    }
    else if (command == "LPUSH") {
        handler.handleLPush(fd, requestArray);
        master->propagateWrite(cmdArgs);
    }
    else if (command == "RPUSH") {
        handler.handleRPush(fd, requestArray);
        master->propagateWrite(cmdArgs);
    }
    else if (command == "LRANGE") {
        handler.handleLRange(fd, requestArray);
    }
    else if (command == "HSET") {
        handler.handleSet(fd, requestArray);
        master->propagateWrite(cmdArgs);
    }
    else if (command == "REPLICA") {  // add replica
        if (master && requestArray.size() >= 3) {
            std::string host = requestArray[1].value;
            int port = std::stoi(requestArray[2].value);
            master->addReplica(host, port);
            std::string response = "+OK\r\n";
            send(fd, response.c_str(), response.length(), 0);
        } else {
            std::string response = "-ERR invalid REPLICA command or not a master\r\n";
            send(fd, response.c_str(), response.length(), 0);
        }
    }
    else if (command == "REPLICAS") {
        std::string info = "Connected replicas: " + std::to_string(master->getConnectedReplicaCount()) + "\n";
        info += master->getMasterInfo() + "\n";
        
        auto replicaList = master->getReplicaList();
        for (const auto& replica : replicaList) {
            info += "- " + replica.first + ":" + std::to_string(replica.second) + "\n";
        }
        
        std::string response = "$" + std::to_string(info.length()) + "\r\n" + info + "\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
    else if (command == "INFO" || command == "REPLCONF" || command == "PSYNC" || command == "WAIT") {
        master->handleReplicationCommand(fd, requestArray);
    }
    else {
        std::string response = "-ERR unknown command\r\n";
        send(fd, response.c_str(), response.length(), 0);
    }
}

void handle_requests(int fd)
{
  Handler handler;
  RESPParser parser;
  std::string buffer;  // dynamic buffer so can receive longer messages
  char temp[BUFFER_SIZE]; // temporary buffer for immediate receive with recv
  MasterServer * master;

  while (true)
  {
    memset(temp, 0, sizeof(temp));
    int bytes_received = recv(fd, temp, sizeof(temp)-1,0);

    if (bytes_received <= 0)
    {
      std::cerr << "Client disconnected or error occurred.\n";
      break;
    }
    
    buffer.append(temp, bytes_received);

    try {
          RESPElement request = parser.parse(buffer);
          if (request.type == RESPType::Array && !request.array.empty()) {
              std::string command = request.array[0].value; // First element is the command
              processRequest(fd, request, handler, master);
              buffer.clear();
          }
        } 
      catch (const std::exception& e) {
        std::string errorMessage = e.what();
        if (errorMessage.find("Incomplete") != std::string::npos) {
            continue;  // wait for rest of msg
        }
        else {
            std::cerr << "RESP Parsing Error: " << errorMessage << "\n";
            send(fd, "-ERR invalid request\r\n", 22, 0);
            buffer.clear();  // move on since parser will never decipher it
        }
      }
  }
}

int masterServerLoop(int port) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    // reuse address so we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port " << port << "\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    std::cout << "Master server waiting for clients to connect on port " << port << "...\n";

    while (true) {  // keep on creating thread per client that forms conn
        int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "TCP Handshake failed\n";
            continue; 
        }
        
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(client_addr.sin_addr), client_ip, INET_ADDRSTRLEN);
        std::cout << "Client connected from " << client_ip << ":" << ntohs(client_addr.sin_port) << "\n";

        std::thread client_thread(handle_requests, client_fd);
        client_thread.detach();  // let it run independently
    }
    close(server_fd);
    return 0;
}

int main(int argc, char* argv[]) {
    bool isReplica = false;
    std::string replicaOfHost;
    int replicaOfPort = 0;
    int port = 6379; // Default port if not provided.
    std::vector<std::pair<std::string, int>> replicaPorts; // List of replica host:port pairs
    MasterServer * master;
    // Simple command-line argument parsing.
    // If "--replicaof <host> <port>" is provided, we run as a replica.
    // The "--port" flag sets the local listening port.
    // New: "--replica <host> <port>" can be used multiple times to add initial replicas
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--replicaof" && i + 2 < argc) {
            isReplica = true;
            replicaOfHost = argv[i + 1];
            replicaOfPort = std::stoi(argv[i + 2]);
            i += 2;
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[i + 1]);
            ++i;
        } else if (arg == "--replica" && i + 2 < argc) {
            std::string host = argv[i + 1];
            int replicaPort = std::stoi(argv[i + 2]);
            replicaPorts.emplace_back(host, replicaPort);
            i += 2;
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            return 1;
        }
    }
    
    if (isReplica) {
        std::cout << "Starting replica instance on port " << port << std::endl;
        std::cout << "Replica connecting to master at " << replicaOfHost 
                  << ":" << replicaOfPort << std::endl;
        auto replica = new ReplicaConnection(port, replicaOfHost, replicaOfPort); 
    } else {
        std::cout << "Starting master server instance on port " << port << std::endl;
        
        master = new MasterServer(port);  // to keep track of replicas + do replica-master responses
        
        for (const auto& replica : replicaPorts) {
            // still need to start up these processes themselves
            std::cout << "Adding initial replica at " << replica.first << ":" << replica.second << std::endl;
            master->addReplica(replica.first, replica.second);
        }
        
        std::cout << "Master ID: " << master->getMasterInfo() << std::endl;
        std::cout << "Connected replicas: " << master->getConnectedReplicaCount() << std::endl;
        
        // Run the server loop
        masterServerLoop(port);
        
        // Clean up
        delete master;
    }
    return 0;
}