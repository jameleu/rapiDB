#include "resp_parser.hpp"
#include <stdexcept>
#include <iostream>
#include <stdexcept>
#include <iostream>

std::string RESPParser::readUntilCRLF(const std::string& input) {
    // Read until the next "\r\n" starting from pos.
    size_t end = input.find("\r\n", this->pos);
    if (end == std::string::npos) {
        // Syntax error: CRLF missing
        throw std::runtime_error("Syntax error: Missing CRLF");
    }
    std::string result = input.substr(this->pos, end - this->pos);
    this->pos = end + 2; // Skip past "\r\n"
    return result;
}

RESPElement RESPParser::parseRESP(const std::string& input) {
    if (this->pos >= input.size()) {
        throw std::runtime_error("Incomplete message: Unexpected end of input");
    }

    char type = input[this->pos++];  // get symbol
    RESPElement elem;

    switch (type) {
        case '+': {  // Simple String, first value read is your string (after advancing pos for symbol)
            elem.type = RESPType::SimpleString;
            elem.value = readUntilCRLF(input);
            break;
        }
        case '-': {  // Error
            elem.type = RESPType::Error;
            elem.value = readUntilCRLF(input);
            break;
        }
        case ':': {  // Integer
            elem.type = RESPType::Integer;
            elem.value = readUntilCRLF(input);
            break;
        }
        case '$': {  // Bulk String
            elem.type = RESPType::BulkString;
            // get full header with read until \r\n, know length now from header
            int length = std::stoi(readUntilCRLF(input));
            if (length == -1) {
                elem.type = RESPType::Null;
                break;
            }
            // Check if the input has enough characters for the bulk string.
            if (this->pos + length > input.size()) {
                throw std::runtime_error("Incomplete message: Expected bulk string of length " 
                                           + std::to_string(length) + " bytes, but not all bytes received.");
            }
            // read after \r\n after $number header
            elem.value = input.substr(this->pos, length);
            this->pos += length;
            break;
        }
        case '*': {  // Array
            elem.type = RESPType::Array;
            int count = std::stoi(readUntilCRLF(input));
            for (int i = 0; i < count; i++) {
                if (this->pos >= input.size()) {
                    throw std::runtime_error("Incomplete message: Expected " + std::to_string(count) 
                                               + " array elements, but fewer elements found.");
                }
                elem.array.push_back(parseRESP(input));
            }

        }
        default:
            throw std::runtime_error("Syntax error: Unknown RESP type");
    }
    // Verify that item is terminated with CRLF.
    if (input.substr(this->pos, 2) != "\r\n") {
        throw std::runtime_error("Incomplete message: Bulk string not terminated properly with CRLF");
    }
    this->pos += 2; // Skip the trailing CRLF.

    return elem;
}

RESPElement RESPParser::parse(const std::string& input) {
    this->pos = 0;  // reset this->position for new reading
    return parseRESP(input);
}
