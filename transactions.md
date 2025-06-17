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