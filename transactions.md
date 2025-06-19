## Adding transactions functionality

### 16.06.2025
Target: The INCR command

Result: Finished adding simple logic in CommandDispatcher new handleIncrCommand method for 

1) Key exists and has a numerical value

2) Key doesn't exist

3) Key exists but doesn't have a numerical value

### 17.06.2025
1) Handle The MULTI command - Just return +OK\r\n for now

2) Handle The EXEC command - Return simple error for now: ERR EXEC without MULTI

3) Empty transaction. Add a logic in connection context to have transaction commands. For now, empty array is returned for EXEC

### 18.06.2025
Target: Queueing commands

Result: In CommandDispatcher before handling any command I am now checking if a context in in transaction mode. If it is, I queue the command and return QUEUED message

Target: Executing a transaction

Result: Added execution of commands. Bulk array formatting is failing now. GET returns a string by default, but a test case is expecting a number

### 19.06.2025
Target: Executing a transaction

Result: Refactored by creating different classes for each data type in redis. Finished transactions and returning bulk array as a response


