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

/*	Helper function that can be passed to the pthread_create to call the map_fn
 */
static void *map_wrapper(void* map_args) {
  struct args_helper *args = (struct args_helper *) map_args;
  args->mr->mapfn_failed[args->id] = args->map(args->mr, args->infd, args->id, args->nmaps);

  pthread_exit((void*) &args->mr->mapfn_failed[args->id]);
  //return (void *)map_args;
}

/*	Helper function that can be passed to the pthread_create to call the reduce_fn
 */
static void *reduce_wrapper(void* reduce_args) {
  struct args_helper *args = (struct args_helper *) reduce_args;
  args->mr->reducefn_failed = args->reduce(args->mr, args->outfd, args->nmaps);

  pthread_exit((void*) &args->mr->reducefn_failed);
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

    mr->infd[i] = open(inpath, O_RDONLY, 644);  // assign different fd to every map thread
    if (mr->infd[i] ==  -1) return -1;

    map_args = &(mr->args[i]);
    map_args->mr = mr;
    map_args->map = mr->map;
    map_args->reduce = mr->reduce;
    map_args->infd = mr->infd[i];
    map_args->id = i;
    map_args->nmaps = mr->n_threads;

		mr->map_thread_failed[i] = pthread_create(&mr->map_threads[i], NULL, &map_wrapper, (void *)map_args);
    if (mr->map_thread_failed[i] != 0)
      return -1;
	}

  // Create a thread for reduce function

  mr->outfd = open(outpath, O_WRONLY | O_CREAT | O_TRUNC, 644);
  if (mr->outfd == -1) return -1;

  reduce_args = &(mr->args[mr->n_threads]);
  reduce_args->mr = mr;
  reduce_args->reduce = mr->reduce;
  reduce_args->map = mr->map;
  reduce_args->outfd = mr->outfd;
  reduce_args->nmaps = mr->n_threads;

	mr->reduce_thread_failed =  pthread_create(&mr->reduce_thread, NULL, &reduce_wrapper, (void *)reduce_args);
  if (mr->reduce_thread_failed != 0)
    return -1;

	return 0;
}

struct map_reduce*
mr_create(map_fn map, reduce_fn reduce, int threads) {
    // http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc

    struct map_reduce *mr = malloc (sizeof(struct map_reduce));
    if(mr == NULL) return NULL;

    pthread_mutex_init(&mr->_lock, NULL);
		mr->map = map;// Save the function inside the sturcture
		mr->reduce = reduce;
		mr->n_threads = threads;// Save the static data
    mr->count = 0;
    mr->size = 0;

    // give meaningless init value
    mr->outfd = -1;
    mr->reducefn_failed = -1;
    mr->reduce_thread_failed = -1;
    mr->outfd_failed = -1;

    mr->buffer = malloc (MR_BUFFER_SIZE); // Create buffer
    if (mr->buffer == NULL) {
      free(mr);
      return NULL;
    }
    mr->args = malloc (sizeof(struct args_helper) * (threads + 1));
    if(mr->args == NULL) {
      free(mr->buffer);
      free(mr);
      return NULL;
    }

    mr->infd = malloc (sizeof(int) * threads);
    if(mr->infd == NULL) {
      free(mr->args);
      free(mr->buffer);
      free(mr);
      return NULL;
    }

    mr->map_threads = malloc(sizeof(pthread_t) * threads);
    if(mr->map_threads == NULL) {
      free(mr->infd);
      free(mr->args);
      free(mr->buffer);
      free(mr);
      return NULL;
    }

    mr->mapfn_failed = malloc(sizeof(int) * threads);
    if(mr->mapfn_failed == NULL) {
      free(mr->map_threads);
      free(mr->infd);
      free(mr->args);
      free(mr->buffer);
      free(mr);
      return NULL;
    } else {
      for(int i=0; i<threads; i++)
        mr->mapfn_failed[i] = -1;
    }

    mr->map_thread_failed = malloc(sizeof(int) * threads);
    if(mr->map_thread_failed == NULL) {
      free(mr->mapfn_failed);
      free(mr->map_threads);
      free(mr->infd);
      free(mr->args);
      free(mr->buffer);
      free(mr);
      return NULL;
    } else {
      for(int i=0; i<threads; i++)
        mr->map_thread_failed[i] = -1;
    }

    mr->infd_failed = malloc(sizeof(int) * threads);
    if(mr->infd_failed == NULL) {
      free(mr->map_thread_failed);
      free(mr->mapfn_failed);
      free(mr->map_threads);
      free(mr->infd);
      free(mr->args);
      free(mr->buffer);
      free(mr);
      return NULL;
    } else {
      for(int i=0; i<threads; i++)
        mr->infd_failed[i] = -1;
    }

		return mr;
}

void
mr_destroy(struct map_reduce *mr) {
  free(mr->infd_failed);
  free(mr->map_thread_failed);
  free(mr->mapfn_failed);
  free(mr->map_threads);
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
mr_finish(struct map_reduce *mr) {

    // close threads
    for(int i=0; i<(mr->n_threads); i++)
        if (mr->map_thread_failed[i] == 0) //success
          pthread_join(mr->map_threads[i], NULL);

    if(mr->reduce_thread_failed == 0) // success
      pthread_join(mr->reduce_thread, NULL);

    // close fd
    for(int i=0; i<(mr->n_threads); i++)
      mr->infd_failed[i] = close(mr->infd[i]);
    mr->outfd_failed = close(mr->outfd);

    // check if success
    if (mr->outfd_failed == -1 || mr->reducefn_failed != 0)
      return -1;

    for(int i=0; i<(mr->n_threads); i++){
        if (mr->infd_failed[i] == -1 || mr->mapfn_failed[i] != 0)
          return -1;  // failed
    }

    return 0; //success
  //check array
  //check pthread join
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
  struct kvpair my_kv;
  my_kv.key = kv->key;
  my_kv.value = kv->value;
  my_kv.keysz = kv->keysz;
  my_kv.valuesz = kv->valuesz;
  int kv_size = kv->keysz + kv->valuesz;

  pthread_mutex_lock(&mr->_lock);
  while(mr->size == MR_BUFFER_SIZE) //from the Lecture 30,31 demo code
      pthread_cond_wait (mr->not_full, &mr->_lock);

  if(mr->size < MR_BUFFER_SIZE){
    mr->buffer[mr->count] = my_kv;
    mr->size+=kv_size;
    mr->count++;
  }

  pthread_cond_signal (mr->not_empty);//from demo code
  pthread_mutex_unlock(&mr->_lock);
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
 *     responsible for allocating memory for the key and value a of time and
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
  struct kvpair my_kv;
  my_kv.key = kv->key;
  my_kv.value = kv->value;
  my_kv.keysz = kv->keysz;
  my_kv.valuesz = kv->valuesz;
  int kv_size = kv->keysz + kv->valuesz;

//This allows write to compile:  const void *buf;

  pthread_mutex_lock(&mr->_lock);
  while(mr->size == 0); //from lecture demo code
    pthread_cond_wait(mr->not_empty, &mr->_lock);

  if(mr->count == 0) return 0;
  else {

//    memset(mr, 0, 1);//I don't know if this will work
                             //http://stackoverflow.com/questions/5844242/valgrind-yells-about-an-uninitialised-bytes
    write(mr->outfd, &mr->buffer[mr->count], 1);
//    write(mr->outfd, &buf, 1); // write to file
    mr->size -= kv_size;
    mr->count--;
  }
  pthread_cond_signal (mr->not_full);//from demo code
  pthread_mutex_unlock(&mr->_lock);
  //	kv->key;
	return 1; // successful
	return -1; // on error
}
