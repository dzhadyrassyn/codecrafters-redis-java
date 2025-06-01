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