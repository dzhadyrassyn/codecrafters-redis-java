## Adding streams functionality

### 25.05.2025
Target: Add The TYPE command

Result: added The TYPE command

### 26.05.2025
Target: Add XADD command processing 

Added simple StreamStorage that saves a stream and one entry. Probably need to refactor the data structure late

### 27-28.05.2025
Target: Add validation for entry ids

Refactored my StreamStorage to become cleaner

Target reached. Added validation for stream id ms-sequence

### 29.05.2025
Target: Solve Partially auto-generated IDs

Result: Implemented the logic with some refactoring

### 30.05.2025
Target: Fully auto-generated IDs

Result: Added implementation. First I generated currentTime-* format, then called previous implementation of replacing sequence number

### 31.05.2025
Target: Add refactoring to StreamStorage

Result: Separated id to a separate class StreamId(timestamp, sequence). Refactored Stream to use StreamId. Changes StreamEntry to use StreamId as a key. Changed Stream to be more thread safe

### 01.06.2025
Target: Finish Query entries from stream

First, investigate the requirements

Implemented the logic that fetches entries between request ranges. Also refactored the code, making StreamId immutable, and adding factory methods

### 02.06.2025
Target: Add Query with -, Query with +

Result. Finish adding query with start time for xrange as "-" and end time as "+"
Simply changed the parsing of StreamId to make a timestamp of 0 for "-" and System.currentTimeMillis() for "+"

### 03.06.2025
Target: Query single stream using XREAD and Query multiple streams using XREAD

First. Investigate the requirements

Result. Finished adding XREAD command with multiple streams. Basically, I reused code snippets from xrange command, doing a search for each stream name separately, and after that formatting the response also using the previous formatting for xrange

### 05.06.2025
Target: Start blocking reads

First, investigate the requirements

### 06.06.2025
Start blocking

### 08.06.2025
Result: Added simple block reading mechanism. Simple Thread.wait(blockTime) is enough for now 

### 09.06.2025
Blocking reads without timeout

First, investigate the requirements

Result: Added lock mechanism. For any block time, the thread is waiting until a notify is called after the add method

Next: The last point for streams
Blocking reads using $

Result: Before fetching by xRead, I read the last added stream-id, save it, and use that for fetching entries after the saved streamId