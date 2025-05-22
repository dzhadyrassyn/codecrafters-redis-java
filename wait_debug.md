### 20.05.2025
Point A - tester::#NA2 is failing 
Passed: first WAIT Test


Failing on
[tester::#NA2] [test] Testing Replica : 1

[tester::#NA2] [test] replica-1: Expecting "SET baz 789" to be propagated

[tester::#NA2] [test] replica-1: Received bytes: "+OK\r\n"

[tester::#NA2] [test] replica-1: Received RESP simple string: "OK"

[tester::#NA2] Expected array type, got SIMPLE_STRING

[tester::#NA2] Test failed

Target: solve the root cause of the bug and solve it

Something is returning +OK, instead of expected array type

In the first test case, SET command is propagates, while in the second one, somehow SET is not propagated in the replica? How that can be possible?

### 21.05.2025
Attempt: locally reproduce the issue

Reproduced locally, found an issue, works fine locally, but not passing test?

Removed writing OK for REPLCONF ACK in server, passed the test! We have the progress. Another test is failing, that's need to be fixed next time

### 22.05.2025
#YD3 test case if failing 

Target: Fix static wait command processing, cause now it only sets 50 as ack value

Result: fixed, test is finally passing!!! 

For the next: I would like to refactor the code, remove unnecessary logs