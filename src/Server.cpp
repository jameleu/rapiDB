#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <thread>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "resp_parser.hpp"
#include "Handler.hpp"
#define BUFFER_SIZE 128


void handle_requests(int fd)
{
  Handler handler;
  RESPParser parser;
  std::string buffer;  // dynamic buffer so can receive longer messages
  char temp[BUFFER_SIZE]; // temporary buffer for immediate receive with recv
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
              // processRequest(fd, request, handler);
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
int main(int argc, char **argv) {
  // Flush
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
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  std::cout << "Client connected\n";
  // std::string pong = "+PONG\r\n";  // + is simple string in Redis, carriage return, new line
  while (true) {  // keep on creating thread per client that forms conn
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    if (client_fd < 0) {
      std::cerr << "TCP Handshake failed\n";
      continue; 
    }
    std::cout << "Client connected\n";

    std::thread client_thread(handle_requests, client_fd);
    client_thread.detach();  // let it run independently
  }

  close(server_fd);

  return 0;
}
