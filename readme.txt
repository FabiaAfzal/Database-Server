Server run by Following commands:
make
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:lib
./server1 -p 8081

Run multiple clients using:
Example:
telnet localhost 8081

check zombie:
ps ax | grep server

