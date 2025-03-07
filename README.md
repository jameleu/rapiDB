# RapiDB
Wanted to dig deeper into how Redis worked. Therefore, I made a replica of it by looking at the docs and doing research on how Redis works. This project is the result of that.
## Benchmarking
My RapiDB
```
 redis-benchmark -p 2000 -t set,get, -n 100000 -q
SET: 204,000.38 requests per second
GET: 209,200.12 requests per second
```
Official Redis Benchmark:
```
redis-benchmark -t set,get, -n 100000 -q
SET: 239,120.26 requests per second
GET: 245,440.54 requests per second
```
## Design Philosophy
* rapidB uses one thread per client connection, whereas Redis uses single thread for event loop
  * I did this so that way could be parallelized
  * Also less logic for queuing and async
  * Also adds locking needed to prevent race condition
* rapiDB supports persistence to disk but writes it in human readable format (txt) instead of binary. Does so in rapiDB home folder, or specified folder
  * May consider using rdb one day
## Supported Methods
**Db stores either key : value or key : list**
Replicas can only handle GET, EXISTS, LRANGE, INFO 
Rest are master only (writes, save, wait)

* SET: Set the value, not list, of a key.
  * Example: SET key value

* GET: Retrieve the value of a key.
  * Example: GET key

* EXISTS: Check if a key exists.
  * Example: EXISTS key

* DEL: Delete a key.
  * Example: DEL key

* DECR: Decrement the integer value of a key by 1.
  * Example: DECR key

* INCR: Increment the integer value of a key by 1.
  * Example: INCR key

* LRANGE: Get a range of elements from a list.
  * Example: LRANGE key start stop

* SAVE: Synchronously save the dataset to disk.
  * Example: SAVE

* LPUSH: Push one or more values to the left of a list.
  * Example: LPUSH key value1 value2 ...


* RPUSH: Push one or more values to the right of a list.
  * Example: RPUSH key value1 value2 ...

* INFO: Get information and statistics about the Redis server.
* WAIT: Wait for the synchronous replication to reach the specified number of replicas.


** Supported Protocols (not callable by client) **
* PSYNC: A protocol command used for partial resynchronization between the master and replica.

* REPLCONF: A command used for configuring replication settings for a replica.


**Arguments:** 
* server  -> program name (entry point)
* "--replicaof <host> <port>" is provided -> run as replica
* "--port" flag sets the local listening port.
* "--replica <host> <port>" can be used multiple times to add initial replicas


## Challenges
* Dependency Management: Dealing with vcpkg dependencies and configuring the toolchain properly was finicky, requiring knowledge of CMake flags and syntax.
* Networking and Socket Programming: Had to learn how to configure sockets, bind to a port, listen for connections, and handle TCP communication.
* Parser: Needed to figure out RESP. Also how to escape characters without adding excessive complexity. Did 
* Buffer Handling: Receiving data with recv required handling dynamic buffers and ensuring correct memory management, as it doesn’t add null terminators, which made string manipulation more difficult.
* OOP, Concurrency and Thread Safety: Challenge was how to represent DB. Researched, and with OOP, can use singleton. Encountered issues with concurrency while using a Singleton pattern for managing the database. Locking mechanisms (e.g., shared and scoped locks) were needed to prevent data races and deadlocks.
* Data Storage Design: Needed to represent single value and lists in DB. How to do that with static types in C++? Could either do polymorphism or Strategy Pattern, but both require too much coding and did not need flexibility for only 2 types of data. Just made two hash maps.
* Error Handling and Function Design: Required heavy validation and research to adhere to Redis protocol.
* Expiry Handling: Representing expiry in a Redis-like system involved careful use of locks and calls to check expiry to avoid race conditions and excess memory and cpu usage.
