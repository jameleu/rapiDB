#ifndef RESP_PARSER_HPP
#define RESP_PARSER_HPP

#include <string>
#include <vector>

enum class RESPType {
    SimpleString,
    BulkString,
    Integer,
    Array,
    Error,
    Null
};

struct RESPElement {
    RESPType type;
    std::string value;            // Used for simple strings, errors, and bulk strings
    std::vector<RESPElement> array; // Used if Array
    int64_t intValue = 0;           // Used for Integer type
    RESPElement() = default;
    explicit RESPElement(RESPType t) : type(t) {}
    explicit RESPElement(int64_t num) : type(RESPType::Integer), intValue(num) {}
};

class RESPParser {
public:
    RESPElement parse(const std::string& input);
    
private:
    size_t pos = 0;
    std::string readUntilCRLF(const std::string& input);
    RESPElement parseRESP(const std::string& input);
};

#endif // RESP_PARSER_HPP
