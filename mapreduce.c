/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.  For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */


/* Header includes */
#include <stdlib.h>

#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024


/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce*
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	// TODO: init an mapreduce struct.
	int fd[2] = {0,1}; // 0 for reading. 1 is for writing
	// TODO: create a buffer
  struct map_reduce* my_mr = (map_reduce*) malloc (sizeof());
	for(int i=0; i<threads, i++){ // TODO: need change
  	map(my_mr, fd[0], i, threads);
	}
	// TODO: call the reduce function
	// TODO: let them conmunicate through the buffer
	// return my_mr;
	// TODO: return NULL; // on failure
}

/* Destroys and cleans up an existing instance of the MapReduce framework
 * Any resources which were acquired or created in mr_create should be released or destroyed here.
 */
void
mr_destroy(struct map_reduce *mr)
{
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	// TODO: create threads  |
	// TODO: setup buffer    | do these 3 in parallel
	// TODO: sync            |
	// TODO: open the inpath
	// TODO: check the outpath
	// TODO: write the outpath

	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	return 0; // if every M&R callback returned 0
	// TODO: else return -1
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	return 1; // successful
	return -1; // on error
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	return 1; // successful
	return 0; // returns without producing another pair
	return -1; // on error
}
