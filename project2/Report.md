Ben Yavin (by240) and Brandon Huang (bh626)

sgd_steps_per_sec=19605 

^
this number was obtained by running the test for 50 times and picking out the best result. ran on grep.cs.rutgers.edu

AI Usage: I (Brandon) used Claude for syntax in Part 1 and tests. Below are the following prompts I used

"Will there be overlap with the Xids if I use getpid and clerkCount?" 
(I was asking for the best way to set xids. I ended up just using clerkCount with bit shifting <<32 instad of os.getpid())

"What is the complete syntax for registering a server? How would this look in a helper method"

"What are the ideas and syntax to implement the following cases in Go:
Retrying a Put after a lost reply does not apply the operation twice. 
Concurrent clerks do not corrupt the store.
A write is reflected in the backup's state before the client sees the reply. 
The backup's full key-value state matches the primary's after a burst of writes."