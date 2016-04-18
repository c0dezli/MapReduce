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
#include <stdlib.h>
#include "mapreduce.h"

/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

typedef struct {									// The args for map function
 struct map_reduce *mr;
 int infd, nmaps, id;
}map_args;

typedef struct {							// The args for reduce function
  struct map_reduce *mr;
  int outfd;
  int nmaps;
}reduce_args;

/*	Helper function that can be passed to the pthread_create to call the map_fn
 */
static void *map_wrapper(void* arg) {
 map_args *args = (map_args *) arg;

 struct map_reduce *mr = args->mr; // Get mr struct pointer
 int infd = args->infd,						 // Get arguments
		 id = args->id,
		 nmaps = args->nmaps;

 int ret = mr->map(mr, infd, id, nmaps); // call function (HOW TO RETURN?????)

 pthread_exit((void*) &ret);
}

/*	Helper function that can be passed to the pthread_create to call the reduce_fn
 */
static void *reduce_wrapper(void* arg) {
 reduce_args *args = (reduce_args *) arg;

 struct map_reduce *mr = args->mr;   // Get mr struct pointer
 int outfd = args->outfd,						 // Get arguments
		 nmaps = args->nmaps;

 int ret = mr->reduce(mr, outfd, nmaps); // call function TODO (HOW TO RETURN)

 pthread_exit((void*) &ret);
}

static int mr_printer(struct map_reduce *mr) {

  printf("lock is set\n");							// Create the lock

  if(mr->myBuffer != NULL)
    printf("Buffer is set\n");       						// Create the buffer
  if(mr->map != NULL)
    printf("Map function pointer is set\n");										// Declear the function pointers
  if(mr->reduce != NULL)
    printf("Reduce function pointer is set\n");

  printf("n_threads value is %d\n", mr->n_threads);             				// Number of worker threads to use
  printf("count value is %d\n", mr->count);// counts bytes in buffer

  if(mr->map_args != NULL)
    printf("map_args is set\n" );
  if(mr->reduce_args != NULL)
    printf("reduce_args is set\n" );

  return 0;
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
  mr_printer(mr);
  // Create n threads for map function (n = n_threads)
	for(int i=0; i<(mr->n_threads); i++) {
    // TODO: ?? mr->map_args = (void *) malloc (sizeof(map_args));
  	map_args args_ins; // The instance

		args_ins.mr = mr;
		args_ins.infd = open(inpath, O_RDONLY);
		args_ins.nmaps = mr->n_threads;
		args_ins.id = i;

		mr->map_args = &args_ins;					// assign the instance to the pointer

		pthread_t c;
		pthread_create(&c, NULL, map_wrapper, mr->map_args);
	}

  // Create a thread for reduce function
  // TODO: ??  mr->reduce_args = (void *) malloc (sizeof(reduce_args));
	reduce_args args_ins; // The instance

	args_ins.mr = mr;
	args_ins.outfd = open(outpath, O_CREAT);    // w+ means if exists, overwrite, else create
	args_ins.nmaps = mr->n_threads;

	mr->reduce_args = &args_ins;					      // assign the instance to the pointer
	pthread_t c;
	pthread_create(&c, NULL, reduce_wrapper, mr->reduce_args);
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
//there is no way to free my_mr because it is a local variable.TODO  We need to make it into something that is passed around like *mr
		struct map_reduce *mr = (struct map_reduce *) malloc (sizeof(struct map_reduce)); //TODO ?

    mr->lock = PTHREAD_MUTEX_INITIALIZER;
		mr->map = map;// Save the function inside the sturcture
		mr->reduce = reduce;
		mr->n_threads = threads;// Save the static data
		mr->myBuffer = (char *) malloc (MR_BUFFER_SIZE); // Create buffer    									// Assign the instance to pointer
    mr->map_args = (void *) malloc (sizeof(map_args));
  	mr->reduce_args = (void *) malloc (sizeof(reduce_args));

		return my_mr;
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
