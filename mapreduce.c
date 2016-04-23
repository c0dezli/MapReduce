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

// The args for map function
struct args_helper {
 struct map_reduce *mr;
 int infd, outfd, nmaps, id;
 map_fn map;
 reduce_fn reduce;
};

/* Helper function that can be passed to the pthread_create to call the map_fn
 */
static void *map_wrapper(void* map_args) {
  struct args_helper *args = (struct args_helper *) map_args;
  args->mr->mapfn_status[args->id] =
      args->map(args->mr, args->infd, args->id, args->nmaps);
  return NULL;
}

/* Helper function that can be passed to the pthread_create to call the reduce_fn
 */
static void *reduce_wrapper(void* reduce_args) {
  struct args_helper *args = (struct args_helper *) reduce_args;
  args->mr->reducefn_status =
    args->reduce(args->mr, args->outfd, args->nmaps);
  return NULL;
}

/*
Refs:
http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc
*/
struct map_reduce*
mr_create(map_fn map, reduce_fn reduce, int threads) {
   struct map_reduce *mr = malloc (sizeof(struct map_reduce));

   if(mr == 0) {  // Check Success
     free(mr);
     return NULL;
   } else {
   // Save the Parameters
   mr->map = map;           // Save the function inside the sturcture
   mr->reduce = reduce;
   mr->n_threads = threads; // Save the static data

   // File Descriptors
   mr->outfd = -1;
   mr->outfd_failed = -1;
   mr->infd = malloc (threads * sizeof(int));
   mr->infd_failed = malloc(threads * sizeof(int));

   // Threads
   mr->map_threads = malloc(threads * sizeof(pthread_t));

   mr->mapfn_status = malloc(threads * sizeof(int));
   mr->reducefn_status = -1;

   // Arguments of Funtion Wappers
   mr->args = malloc ((threads + 1) * sizeof(struct args_helper));

   // Lock & Conditional Variables
   mr->_lock = malloc(threads * sizeof(pthread_mutex_t));
   mr->not_full = malloc(threads * sizeof(pthread_cond_t));
   mr->not_empty = malloc(threads * sizeof(pthread_cond_t));

   for (int i=0; i<threads; i++) {  // Init
       pthread_mutex_init(&mr->_lock[i], NULL);
       pthread_cond_init(&mr->not_full[i], NULL);
       pthread_cond_init(&mr->not_empty[i], NULL);
   }

   // Init the Buffer List
   mr->buffer = malloc(threads * sizeof(char*));
   mr->size = malloc(threads * sizeof(int));

   for(int i = 0; i < threads; i++){
     mr->buffer[i] = malloc(MR_BUFFER_SIZE * sizeof(char));
     mr->size[i] = 0;
   }
	 return mr;
 }
}

int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath) {
  struct args_helper *map_args,
                     *reduce_args;

  // Create n threads for map function (n = n_threads)
	for(int i=0; i<(mr->n_threads); i++) {
    // Assign different fd to every map thread
    mr->infd[i] = open(inpath, O_RDONLY, 644);
    if (mr->infd[i] == -1) {
      close(mr->infd[i]);
      perror("Cannot open input file\n");
      return -1;
    }

    map_args = &(mr->args[i]);
    map_args->mr = mr;
    map_args->map = mr->map;
    map_args->reduce = mr->reduce;
    map_args->infd = mr->infd[i];
    map_args->id = i;
    map_args->nmaps = mr->n_threads;

		if(pthread_create(&mr->map_threads[i], NULL, &map_wrapper, (void *)map_args) != 0) {
      perror("Failed to create map thread.\n");
      return -1;
    }
	}

  // Create thread for reduce function
  mr->outfd = open(outpath, O_WRONLY | O_CREAT | O_TRUNC, 644);
  if (mr->outfd == -1) {
    close(mr->outfd);
    perror("Cannot open output file\n");
    return -1;
  }

  reduce_args = &(mr->args[mr->n_threads]);
  reduce_args->mr = mr;
  reduce_args->reduce = mr->reduce;
  reduce_args->map = mr->map;
  reduce_args->outfd = mr->outfd;
  reduce_args->nmaps = mr->n_threads;

  if (pthread_create(&mr->reduce_thread, NULL, &reduce_wrapper, (void *)reduce_args) != 0) {
    perror("Failed to create reduce thread.\n");
    return -1;
  }

	return 0;
}

void
mr_destroy(struct map_reduce *mr) {
  for(int i=0; i<mr->n_threads; i++){
    free(mr->buffer[i]);
  }
  free(mr->buffer);

  free(mr->infd);
  free(mr->infd_failed);

  free(mr->map_threads);
  free(mr->mapfn_status);

  free(mr->not_full);
  free(mr->not_empty);
  free(mr->_lock);

  free(mr->size);
  free(mr->args);

  free(mr);
}

int
mr_finish(struct map_reduce *mr) {

  // Close Threads
  for(int i=0; i<(mr->n_threads); i++) {
    if(pthread_join(mr->map_threads[i], NULL)) {
      perror("Failed to wait a map thead end.\n");
      return -1;
    }
  }

  if(pthread_join(mr->reduce_thread, NULL)) {
    perror("Failed to wait a map thead end.\n");
    return -1;
  }

  // Close the File Descriptors
  for(int i=0; i<(mr->n_threads); i++) {
    mr->infd_failed[i] = close(mr->infd[i]);
  }
  mr->outfd_failed = close(mr->outfd);

  // Check
  for(int i=0; i<(mr->n_threads); i++) {
    if (mr->outfd_failed    == -1 ||
        mr->reducefn_status !=  0 ||
        mr->infd_failed[i]  == -1 ||
        mr->mapfn_status[i] !=  0  )
      return -1;
  }

  return 0; //success
}

int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv) {

  pthread_mutex_lock(&mr->_lock[id]); // lock

  int kv_size = kv->keysz + kv->valuesz + 8;
  // first check if the buffer is overflow
  while((mr->size[id]+kv_size) >= MR_BUFFER_SIZE) {
    pthread_cond_wait(&mr->not_full[id], &mr->_lock[id]); // wait
  }

  memmove(&mr->buffer[id][mr->size[id]], &kv->keysz, 4);
	mr->size[id] += 4;

	memmove(&mr->buffer[id][mr->size[id]], kv->key, kv->keysz);
	mr->size[id] += kv->keysz;

	memmove(&mr->buffer[id][mr->size[id]], &kv->valuesz, 4);
	mr->size[id] += 4;

	memmove(&mr->buffer[id][mr->size[id]], kv->value, kv->valuesz);
	mr->size[id] += kv->valuesz;

  pthread_cond_signal (&mr->not_empty[id]); //Send the signal
  pthread_mutex_unlock(&mr->_lock[id]); // unlock failed

	return 1;
}

int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv) {
  pthread_mutex_lock(&mr->_lock[id]); // lock

  // make surewthere is value to consume
  while(mr->size[id] <= 0) {
    if(mr->mapfn_status[id] == 0){  // Map function done its work
      printf("DONE! Consume: ID = %d, mr->size[id] is %d, no more pairs, return 0\n", id, mr->size[id]);
      return 0;
    }
    pthread_cond_wait(&mr->not_empty[id], &mr->_lock[id]); // wait
  }

  int offset = 0;
  memmove(&kv->keysz, &mr->buffer[id][offset], 4);
	offset += 4;
	memmove(kv->key, &mr->buffer[id][offset], kv->keysz);
	offset += kv->keysz;
	memmove(&kv->valuesz, &mr->buffer[id][offset], 4);
	offset += 4;

	memmove(kv->value, &mr->buffer[id][offset], kv->valuesz);
	offset += kv->valuesz;

  // decrease size
  mr->size[id] -= offset;
  memmove(&mr->buffer[id][0], &mr->buffer[id][offset], (MR_BUFFER_SIZE - offset));


  pthread_cond_signal (&mr->not_full[id]);//from demo code
  pthread_mutex_unlock(&mr->_lock[id]); // unlock failed

  //printf("Consume: ID is %d, Count is %d, mr->size[id] is %d, node_size is %d\n", id, mr->count[id], mr->size[id], node_size);
	return 1; // successful
}
