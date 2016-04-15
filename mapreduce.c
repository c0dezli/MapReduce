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

/**
 * Begins a multithreaded MapReduce operation.  This operation will process data
 * from the given input file and write the result to the given output file.
 *
 * mr       Pointer to the instance to start
 * inpath   Path to the file from which input is read.  The framework should
 *          make sure that each Map thread gets an independent file descriptor
 *          for this file.
 * outpath  Path to the file to which output is written.
 *
 * Returns 0 if the operation was started successfuly and nonzero if there was
 * an error.
 */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath) {

	// TODO: create threads  |
	// TODO: setup buffer    | do these 3 in parallel
	// TODO: sync            |

	pthread_t c;
	pthread_create(&c, NULL, mr->map, NULL);

	mr->fd = open(inpath, O_RDONLY); //open the inpath

	if( access(outpath, F_OK) != -1){
		//TODO: file exists
	}
	else {
		//TODO: file doesn't exist
	}

	mr_finish(mr);
	return 0;
}

/**
 * Allocates and initializes an instance of the MapReduce framework.  This
 * function should allocate a map_reduce structure and any memory or resources
 * that may be needed by later functions.
 *
 * map      Pointer to map callback function
 * reduce   Pointer to reduce callback function
 * threads  Number of worker threads to use
 *
 * Returns a pointer to the newly allocated map_reduce structure on success, or
 * NULL to indicate failure.
 */
struct map_reduce*
mr_create(map_fn map, reduce_fn reduce, int threads) {

	int struct_size = MR_BUFFER_SIZE +
	                  sizeof(pthread_mutex_t) +
										3 * sizeof(int);
										//sizeof(map_fn) +
										//sizeof(reduce_fn);

	printf("%d", struct_size);
	
	struct map_reduce* my_mr = (struct map_reduce*) malloc (struct_size);
	my_mr->id = threads;
	//my_mr->map = map;
	//my_mr->reduce = reduce;
	//my_mr->myBuffer = (char *) malloc (MR_BUFFER_SIZE);

	return  my_mr;
}

/**
 * Destroys and cleans up an existing instance of the MapReduce framework.  Any
 * resources which were acquired or created in mr_create should be released or
 * destroyed here.
 *
 * mr  Pointer to the instance to destroy and clean up
 */
void
mr_destroy(struct map_reduce *mr) {
	free(mr->myBuffer);
	free(mr);

}

/**
 * Blocks until the entire MapReduce operation is complete.  When this function
 * returns, you are guaranteeing to the caller that all Map and Reduce threads
 * have completed.
 *
 * mr  Pointer to the instance to wait for
 *
 * Returns 0 if every Map and Reduce function returned 0 (success), and nonzero
 * if any of the Map or Reduce functions failed.
 */
int
mr_finish(struct map_reduce *mr)
{
	mr_destroy(mr);
	return 0; // if every M&R callback returned 0
	// TODO: else return -1
}

/**
 * Called by a Map thread each time it produces a key-value pair to be consumed
 * by the Reduce thread.
 * If the framework cannot currently store another
 * key-value pair, this function should block until it can.
 *
 * mr  Pointer to the MapReduce instance
 * id  Identifier of this Map thread, from 0 to (nmaps - 1)
 * kv  Pointer to the key-value pair that was produced by Map.  This pointer
 *     belongs to the caller, so you must copy the key and value data if you
 *     wish to store them somewhere.
 *
 * Returns 1 if one key-value pair is successfully produced (success), -1 on
 * failure.  (This convention mirrors that of the standard "write" function.)
 */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{

	//	kv->key;
	//	kv->value;
	//	kv->keysz + kv->valuesz = total size;
	return 1; // successful
	return -1; // on error
}

/**
 * Called by the Reduce function to consume a key-value pair from a given Map
 * thread.  If there is no key-value pair available, this function should block
 * until one is produced (in which case it will return 1) or the specified Map
 * thread returns (in which case it will return 0).
 *
 * mr  Pointer to the MapReduce instance
 * id  Identifier of Map thread from which to consume
 * kv  Pointer to the key-value pair that was produced by Map.  The caller is
 *     responsible for allocating memory for the key and value ahead of time and
 *     setting the pointer and size fields for each to the location and size of
 *     the allocated buffer.
 *
 * Returns 1 if one pair is successfully consumed, 0 if the Map thread returns
 * without producing any more pairs, or -1 on error.  (This convention mirrors
 * that of the standard "read" function.)
 */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	return 1; // successful
	return 0; // returns without producing another pair
	return -1; // on error
}
