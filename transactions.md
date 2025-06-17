## Adding transactions functionality

### 16.06.2025
Target: The INCR command

Result: Finished adding simple logic in CommandDispatcher new handleIncrCommand method for 

Key exists and has a numerical value (previous stages)

Key doesn't exist (previous stages)

Key exists but doesn't have a numerical value (This stage)

### 17.06.2025
Handle The MULTI command

Result: Just return +OK\r\n for now