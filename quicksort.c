//compile with gcc -O2 -pthread quicksort.c -o quicksort

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

#define N 4
#define TASKS 1000
#define LEN 1000000
#define LIMIT 250000

int global_availmsg = 0; 
int sorted = 0;
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct thpool_* threadpool;
threadpool thpool_init(int n);

//Message
typedef struct msg{
	double *a;
	int first;
	int last;
	int complete;
	int shutdown;
	void (*function)(void* arg);
	void* arg;
} msg;

//Queue
typedef struct queue{
	int front;
	int rear;
	msg* array;
	int size;
} queue;

//Thread
typedef struct thread{
	int id;
	pthread_t pthread;
	struct thpool_* thpool_p;
} thread;

//Threadpool
typedef struct thpool_{
	thread** threads;
	volatile int num_threads_alive;
	queue queue;
} thpool_;

//Inssort function
void inssort(double *a,int first, int last, queue* queue_p) {
	int i,j;
	double t;
  
	for (i=first+1;i<=last;i++) {
		j = i;
		while ((j>0) && (a[j-1]>a[j])) {
			t = a[j-1];
			a[j-1] = a[j];
			a[j] = t;
			j--;
		}
	}
	msg_create(a, first, last, 1, 0, &queue_p, NULL, NULL);
}

//Partition function
int partition(double*a, int first, int last){
	int middle;
	double t,p;
	int i,j;

	// take middle position
	middle = (last-first)/2;  
  
	// put median-of-3 in the middle
	if (a[middle]<a[first]) { t = a[middle]; a[middle] = a[first]; a[first] = t; }
	if (a[last]<a[middle]) { t = a[last]; a[last] = a[middle]; a[middle] = t; }
	if (a[middle]<a[first]) { t = a[middle]; a[middle] = a[first]; a[first] = t; }
    
	// partition (first and last are already in correct half)
	p = a[last]; // pivot
	for (i=first+1,j=last-2;;i++,j--) {
		while (a[i]<p) i++;
		while (p<a[j]) j--;
		if (i>=j) break;

		t = a[i]; a[i] = a[j]; a[j] = t;      
	}
	return i;
}

//Quicksort function
void quicksort(double* a, int first, int last, int complete, int shutdown, queue* queue_p){
	int i;
	
	if (last - first <= 10){
		inssort(a, first, last, &queue_p);
		return;
	}
	
	i = partition(a, first, last);
	
	msg_create(a, first, i, 0, 0, &queue_p, (void*)quicksort, NULL);
	msg_create(a, first+i, last-i, 0, 0, &queue_p, (void*)quicksort, NULL);
}

//Work function
void *work(struct thread* thread_p){
	/* Assure all threads have been created before starting serving */
	thpool_* thpool_p = thread_p->thpool_p;
	
	void (*func_p)(double*, int*, int*, int*, int*);
	double* arg_a;
	int* arg_first;
	int* arg_last;
	int* arg_complete;
	int* arg_shutdown;
	msg* msg_p = msg_read(&thpool_p->queue);
	

	pthread_mutex_lock(&mutex);
	thpool_p->num_threads_alive += 1;
	pthread_mutex_unlock(&mutex);
	
	if (msg_p){
		func_p = msg_p->function;
		arg_a = msg_p->a;
		arg_first = msg_p->first;
		arg_last = msg_p->last;
		arg_complete = msg_p->complete;
		arg_shutdown = msg_p->shutdown;
		if (arg_complete == 1){
			printf("Sorted from %ls to %ls\n", arg_first, arg_last);
			sorted += arg_last - arg_first;
			if (sorted == LEN){
				//????
			}
			free(msg_p);
			return;
		}
		func_p(arg_a, arg_first, arg_last, arg_complete, arg_shutdown, &thpool_p->queue);
		free(msg_p);
	}
	
	pthread_mutex_lock(&mutex);
	thpool_p->num_threads_alive --;
	pthread_mutex_unlock(&mutex);
}

//Reading a message from queue
static struct msg* msg_read(queue* queue_p){

	pthread_mutex_lock(&mutex);
	
	while (global_availmsg<1) {
		pthread_cond_wait(&msg_in,&mutex); 
	}
	
	msg* msg_p = queue_p->front;

	if (queue_p->size == 0){
		return NULL;
	}
	else if (queue_p->size == 1){
		queue_p->front = NULL;
		queue_p->rear  = NULL;
		queue_p->size = 0;
		return NULL;
	}
	else{
		queue_p->front = (queue->front + 1)%TASKS;;
		queue_p->size = queue_p->size-1;
	}

	global_availmsg = 0;
	
	pthread_cond_signal(&msg_out);
	
	pthread_mutex_unlock(&mutex);
	
	return msg_p;
}

//Creating a message in queue
int msg_create(double *a, int first, int last, int complete, int shutdown, queue* queue_p, void (*function_p)(void*), void* arg_p){
	msg* newmsg;
	
	pthread_mutex_lock(&mutex);
	
    while (global_availmsg>0) {
    	pthread_cond_wait(&msg_out,&mutex); 
	}
	
	newmsg = (struct msg*)malloc(sizeof(struct msg));
	if (newmsg == NULL){
		printf("Could not allocate memory for new message\n");
		return -1;
	}
	
	newmsg->a = a;
	newmsg->first = first;
	newmsg->last = last;
	newmsg->complete = complete;
	newmsg->shutdown = shutdown;
	newmsg->function = function_p;
	newmsg->arg = arg_p;
	
	pthread_mutex_lock(&mutex);
	
	queue_p->rear = (queue_p->rear + 1)%TASKS;
	queue_p->array[queue_p->rear] = newmsg;
	queue_p->size = queue_p->size + 1;
	
	global_availmsg = 1;
	
	pthread_cond_signal(&msg_in);
	
	pthread_mutex_unlock(&mutex);
	
	return 0;
}

//Creating a thread in threadpool
static int thread_init (thpool_* thpool_p, struct thread** thread_p, int id){

	*thread_p = (struct thread*)malloc(sizeof(struct thread));
	if (thread_p == NULL){
		printf("Could not allocate memory for thread\n");
		return -1;
	}

	(*thread_p)->thpool_p = thpool_p;
	(*thread_p)->id = id;
	
	//Add work to thread
	pthread_create(&(*thread_p)->pthread, NULL, (void *)work, (*thread_p));
	
	return 0;
}

//Destrying thread
static void thread_destroy (thread* thread_p){
	free(thread_p);
}

//Creating queue
static int queue_init(queue* queue_p){
	queue_p->size = 0;
	queue_p->front = 0;
	queue_p->rear = TASKS-1;
	queue_p->array = (struct msg*)malloc(TASKS*sizeof(struct msg));
	if (queue_p->array == NULL){
		return -1;
	}
}

//Destroying queue
static void queue_destroy(queue* queue_p){
	while(queue_p->size){
		free(msg_read(queue_p));
	}

	queue_p->front = NULL;
	queue_p->rear  = NULL;
	queue_p->size = 0;
}

//Creating threadpool
struct thpool_* thpool_init(int n){
	//Make threadpool
	thpool_* thpool_p;
	thpool_p = (struct thpool_*)malloc(sizeof(struct thpool_));
	if (thpool_p == NULL){
		printf("Could not allocate memory for threadpool\n");
		return NULL;
	}
	thpool_p->num_threads_alive = 0;
	
	//Make queue
	if (queue_init(&thpool_p->queue) == -1){
		printf("Could not allocate memory for queue\n");
		free(thpool_p);
		return NULL;
	}
	
	//Make threads
	thpool_p->threads = (struct thread**)malloc(n*sizeof(struct thread *));
	if (thpool_p->threads == NULL){
		printf("Could not allocate memory for threads\n");
		queue_destroy(&thpool_p->queue);
		free(thpool_p);
		return NULL;
	}
	
	//Initialize threads
	int i;
	for (i=0; i<n; i++){
		thread_init(thpool_p, &thpool_p->threads[i], i);
		printf("Created thread %d in pool\n", i);
		thpool_p->num_threads_alive++;
	}
	
	//Wait
	while (thpool_p->num_threads_alive != n) {}

	return thpool_p;
}

//Destroying threadpool
void thpool_destroy(thpool_* thpool_p){
	if (thpool_p == NULL){
		return;
	}
	
	queue_destroy(&thpool_p->queue);
	
	int n;
	for (n=0; n < N; n++){
		thread_destroy(thpool_p->threads[n]);
	}
	free(thpool_p->threads);
	free(thpool_p);
}

int main (){ 
	double *a;
	int i;
	queue queue;
	
	a = (double *)malloc(N*sizeof(double));
	if (a==NULL) {
		printf("Could not allocate memory for array a[]\n");
		exit(1);
	}
	
	// fill array with random numbers
	srand(time(NULL));
	for (i=0;i<LEN;i++) {
		a[i] = (double)rand()/RAND_MAX;
	}
	
	threadpool thpool = thpool_init(N);

	msg_create(a, 0, LEN-1, 0, 0, &queue, (void*)quicksort, NULL);
	
	// check sorting
	for (i=0;i<(LEN-1);i++) {
		if (a[i]>a[i+1]) {
			printf("Sort failed!\n");
			break;
		}
	}  

	// free everything
	free(a);
	thpool_destroy(thpool);
	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&msg_out);
	pthread_cond_destroy(&msg_in);
	
	return 0;
}