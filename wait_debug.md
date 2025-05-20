### 20.01.2025
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