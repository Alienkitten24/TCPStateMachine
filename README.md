\# TCP State Machine



Program implementing the Transmission Control Protocol (TCP), including tcp state machine, packets, multiplexing, and loss handling.





Command for testing server: 

```bash

java -DUDPPORT=8816 -DLOSSRATE=0.50 \[server1/server2] 8817

```



Command for testing client: 

```bash

java -DUDPPORT=8816 -DLOSSRATE=0.50 \[client1/client2] \[servermachinename] 8817

```

