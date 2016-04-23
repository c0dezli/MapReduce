# MapReduce

decriptions

# Team members

* Xiang Li xpl5016
* Jesse Rong jbr5205

# Performance evaluation

## Data

|    1 thread  | P       | H      |  A    |  
|---|---|---|---|
|  100B buffer |0.000368 |0.000938 | 30.897 |   
|  1kB buffer  |0.000946 | 0.657   | 31.46  |   
|  10kB buffer |0.000581 | 0.685   | 31.61  |   


|    2 thread   | P         | H       |  A     |  
|---|---|---|---|
|  100B buffer  | 0.000690  | 0.001253| 18.265 |   
|  1kB buffer   | 0.000541  | 0.402   | 18.45  |   
|  10kB buffer  | 0.000882  | 0.545   | 18.21  |   


| 4 thread   | P  | H  |  A |  
|---|---|---|---|
|  100B buffer |  0.000835 | 0.68646| 10.706 |   
|  1kB buffer  |  0.000686 | 0.248  | 10.59  |   
|  10kB buffer |  0.000896 | 0.255  | 10.58  |   

| 8 thread   | P  | H  |  A |  
|---|---|---|---|
|  100B buffer | 0.000873  | 0.408601| 5.96  |   
|  1kB buffer  | 0.000759  | 0.166   | 6.22  |   
|  10kB buffer | 0.001028  | 0.185   | 6.06  |   



## Expectation

We expect that with increasing threads, the program will run faster and then
slower once there are too many threads. Also, the bigger the buffers are, the
faster the program will execute.

We found that our expectations were somewhat correct. For panger, as the number
 of threads increased, the program got slower. For arabian nights, as the 
number of threads increased, the program got faster. For hamlet, the 
program got slower and then faster. As the buffers got bigger, panger
got faster then slower, hamlet got slower then faster, and arabian
nights pretty much stayed the same. 

In addition to the 6 given functions and structure, our c program has 1 more
structure for the wrapper functions. We have 2 extra functions, both are wrapper
functions for map and reduce.

Within the .h file's map_reduce structure, we put a lock, 2 conditional
variables, the head and tail nodes to our linked list, information about the
number and size of key values and file descriptors. The conditional variables and locks were to prevent deadlock. 

In the .c program, we had a lot of trouble with memory. We spent many hours
over several days finding out where and why some memory would not be initialized
or freed. We also experienced problems with the usage of locks.

# Sources consulted
(please cite here as well as in comments by the relevant code)
http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc

https://www.ibm.com/support/knowledgecenter/ssw_aix_71/com.ibm.aix.basetrf1/PTHREAD_MUTEX_INITIALIZER.htm

http://stackoverflow.com/questions/230062/whats-the-best-way-to-check-if-a-file-exists-in-c-cross-platform

http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc

http://stackoverflow.com/questions/143123/how-to-create-a-buffer-for-reading-socket-data-in-c

Lecture 30,31 Demo Code

Code that the professor showed me: test-pthreads.c

# Usage

```
./mr_wordc INPUT_FILE_NAME OUTPUT_FILE_NAME TIME_FILE_NAME NUM_OF_THREAD
```
