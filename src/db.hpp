#ifndef DB_HPP
#define DB_HPP

#include <unordered_map>
#include <string>
#include <vector>
#include <stdexcept>
#include <mutex>

class DB {
public:
    // Get the singleton instance.
    static DB& getInstance();

    // Delete copy constructor and assignment operator.
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Set a key to a string value.
    // Throws if the key already holds a list.
    void set(const std::string& key, const std::string& value);

    // Get the string value of a key.
    // Throws if the key does not exist or if the key holds a list.
    std::string get(const std::string& key);

    // Check if a key exists (in either store).
    bool exist(const std::string& key);

    // Erase (delete) a key from both stores.
    bool erase(const std::string& key);

    // Increment the numeric value stored at key.
    // If key doesn't exist, set it to "1".
    int incr(const std::string& key);

    // Decrement the numeric value stored at key.
    // If key doesn't exist, set it to "-1".
    int decr(const std::string& key);

    // Push a value onto the head of a list.
    // If key does not exist, a new list is created.
    // Throws if the key holds a string.
    void lpush(const std::string& key, const std::string& value);

    // Push a value onto the tail of a list.
    // If key does not exist, a new list is created.
    // Throws if the key holds a string.
    void rpush(const std::string& key, const std::string& value);

    // Return a subset of the list stored at key, between start and stop (inclusive).
    std::vector<std::string> lrange(const std::string& key, int start, int stop);

    // get size of string/list. 0 if does not exist
    size_t sizeOf(const std::string& key);

private:
    DB() {}

    // one for string values, one for list values.
    std::unordered_map<std::string, std::string> stringStore_;
    std::unordered_map<std::string, std::vector<std::string>> listStore_;

    // One mutex per store.
    mutable std::mutex stringMutex_;
    mutable std::mutex listMutex_;
};

#endif // DB_HPP
