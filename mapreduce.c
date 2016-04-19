/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.
 * For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */

/* Header includes */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <fcntl.h>
#include "mapreduce.h"

/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

struct args_helper{									// The args for map function
 struct map_reduce *mr;
 int infd, outfd, nmaps, id;
 map_fn map;
 reduce_fn reduce;
};

static int mr_printer(struct map_reduce *mr) {

  if(mr->buffer != NULL)
    printf("Buffer is set\n");       						// Create the buffer
  if(mr->map != NULL)
    printf("Map function pointer is set\n");										// Declear the function pointers
  if(mr->reduce != NULL)
    printf("Reduce function pointer is set\n");

  printf("n_threads value is %d\n", mr->n_threads);             				// Number of worker threads to use
  printf("count value is %d\n", mr->count);// counts bytes in buffer

  if(mr->args != NULL)
    printf("args is set\n\n\n\n" );

  return 0;
}

/*	Helper function that can be passed to the pthread_create to call the map_fn
 */
static void *map_wrapper(void* arg) {
  struct args_helper *map_args = (struct args_helper *) arg;

  if(map_args->mr != NULL) {
    printf("The arg->mr is set\n");
    mr_printer(map_args->mr);
  }
  printf(" The infd is %d, The nmaps is %d, The id is %d\n\n\n\n", map_args->infd, map_args->nmaps, map_args->id);

  map_args->mr->map_failed[map_args->id] = map_args->map(map_args->mr, map_args->infd, map_args->id, map_args->nmaps);

  pthread_exit((void*) &map_args->mr->map_failed[map_args->id]);
  //return (void *)map_args;
}

/*	Helper function that can be passed to the pthread_create to call the reduce_fn
 */
static void *reduce_wrapper(void* arg) {
  struct args_helper *reduce_args = (struct args_helper *) arg;

  if(reduce_args->mr != NULL) {
    printf("The arg->mr is set\n");
    mr_printer(reduce_args->mr);
  }
  printf(" The out fd is %d, The nmaps is %d\n\n\n\n", reduce_args->outfd, reduce_args->nmaps);

  reduce_args->mr->reduce_failed = reduce_args->reduce(reduce_args->mr, reduce_args->outfd, reduce_args->nmaps);

  pthread_exit((void*) &reduce_args->mr->reduce_failed);
  //return (void *)reduce_args;
}

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

  struct args_helper *map_args,
             *reduce_args;

	for(int i=0; i<(mr->n_threads); i++) {   // Create n threads for map function (n = n_threads)

    mr->infd[i] = open(inpath, O_RDONLY);  // assign different fd to every map thread
    if (mr->infd[i]<0) return -1;

    map_args = &(mr->args[i]);
    map_args->mr = mr;
    map_args->map = mr->map;
    map_args->reduce = mr->reduce;
    map_args->infd = mr->infd[i];
    map_args->id = i;
    map_args->nmaps = mr->n_threads;

		pthread_create(&mr->map_threads[i], NULL, &map_wrapper, (void *)map_args);
	}

  // Create a thread for reduce function

  mr->outfd = open(outpath, O_CREAT);
  if (mr->outfd<0) return -1;

  reduce_args = &(mr->args[mr->n_threads]);
  reduce_args->mr = mr;
  reduce_args->reduce = mr->reduce;
  reduce_args->map = mr->map;
  reduce_args->outfd = mr->outfd;
  reduce_args->nmaps = mr->n_threads;

	pthread_create(&mr->reduce_thread, NULL, &reduce_wrapper, (void *)reduce_args);
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
    // http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc

    struct map_reduce *mr = malloc (sizeof(struct map_reduce));
    if(mr == NULL) return NULL;

    pthread_mutex_init(&mr->_lock, NULL);
		mr->map = map;// Save the function inside the sturcture
		mr->reduce = reduce;
		mr->n_threads = threads;// Save the static data

    mr->count = -1;         // give meaningless init value
    mr->outfd = -1;

    mr->buffer = malloc (MR_BUFFER_SIZE); // Create buffer
    if (mr->buffer == NULL) {
      free(mr);
      return NULL;
    }
    mr->args = malloc (sizeof(struct args_helper));
    if(mr->args == NULL) {
      free(mr->buffer);
      free(mr);
      return NULL;
    }

    mr->infd = calloc (threads, sizeof(int));
    if(mr->infd == NULL) {
      free(mr->args);
      free(mr->buffer);
      free(mr);
      return NULL;
    }

		return mr;
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
  free(mr->infd);
  free(mr->args);
  free(mr->buffer);
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
//check array
//check pthread join
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
