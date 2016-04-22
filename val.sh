
#!/bin/bash

valgrind --leak-check=full --track-origins=yes ./run-tests 2 $1 #all
