#include "org_context.h"
#include "ldb_define.h"
#include "ldb_slice.h"
#include "lmalloc.h"

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <syslog.h>
#include <assert.h>
#include <sys/time.h>
#include <time.h>
#include <sched.h>

org_context_t *testContext = NULL;




#define BEGIN_FUNC  \
	struct timeval begin_tv; \
	gettimeofday(&begin_tv, NULL);

#define END_FUNC  \
	struct timeval end_tv; \
	gettimeofday(&end_tv, NULL); \
	unsigned long long cost_time = (end_tv.tv_sec - begin_tv.tv_sec) * 1000000; \
	cost_time += end_tv.tv_usec - begin_tv.tv_usec; \
	printf("%s begin %u.%llu end %u.%llu cost %llu us\n", __func__, begin_tv.tv_sec, begin_tv.tv_usec, end_tv.tv_sec, end_tv.tv_usec, cost_time);


#define BEGIN_SECTION  \
	struct timeval begin_sec; \
	gettimeofday(&begin_sec, NULL);

#define END_SECTION \
	struct timeval end_sec; \
	gettimeofday(&end_sec, NULL); \
	unsigned long long cost_time = (end_sec.tv_sec - begin_sec.tv_sec) * 1000000; \
	cost_time += end_sec.tv_usec - begin_sec.tv_usec; \
    if (cost_time >= 1) \
	printf("%s SEC begin %u.%u end %u.%u cost %llu us\n", __func__, begin_sec.tv_sec, begin_sec.tv_usec, end_sec.tv_sec, end_sec.tv_usec, cost_time);


#define random(x) (rand()%x)

int TIMES = 2000000;
int NUM = 0;

int arr1[1024] = {0};

int arr2[1024] = {0};

int arr3[1024] = {0};


typedef struct org_value_item_t{
  uint64_t version_;
  size_t data_len_;
  char* data_;
} org_value_item_t;


static ldb_slice_t* org_meta_slice_create(){ 
  char strmeta[sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t)] = {0};
  ldb_slice_t *slice = ldb_slice_create(strmeta, sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t));
  return slice;
}


static void org_encode_kv_key(const char* key, size_t keylen, ldb_slice_t** pslice){
  ldb_slice_t* slice = org_meta_slice_create();
  ldb_slice_push_back(slice, LDB_DATA_TYPE_KV, strlen(LDB_DATA_TYPE_KV));
  ldb_slice_push_back(slice, key, keylen);
  *pslice =  slice;
}

static int org_string_set(org_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value){
  int retval = 0;
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    retval = LDB_ERR;
    goto end;
  }
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  org_encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK;

end:
  return retval;
}


static int org_string_get(org_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue){
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
  ldb_slice_t *slice_key = NULL;
  org_encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  int retval = LDB_OK;
  if(errptr != NULL){
    fprintf(stderr, "leveldb_get fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val != NULL){
    *pvalue = ldb_slice_create(val, vallen);
    retval = LDB_OK;
    leveldb_free(val);
  }else{
    retval = LDB_OK_NOT_EXIST;
  }

end:
  return retval;
}

static int org_set(org_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            uint64_t lastver, 
            int vercare, 
            long exptime, 
            org_value_item_t* item, 
            int en){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val , *slice_value = NULL;
  slice_key = ldb_slice_create(key, keylen);
  slice_val = ldb_slice_create(item->data_, item->data_len_);
  if(en || 1){
    retval = org_string_set(context, slice_key, slice_val);
  }

end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  ldb_slice_destroy(slice_value);

  return retval;
}


static int org_get(org_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            org_value_item_t** item){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val = NULL;
  slice_key = ldb_slice_create(key, keylen);
  retval = org_string_get(context, slice_key, &slice_val);
  if(retval != LDB_OK){
    goto end;
  }
  *item = (org_value_item_t*)lmalloc(sizeof(org_value_item_t));
  retval = LDB_OK; 
end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);

  return retval;
}

void* testSetString(void* no)
{
	BEGIN_FUNC;

	uint32_t cArea = *((int*)no);

	printf("%s %ud %d begin...\n", __func__, cArea, TIMES);

	int iKey = 0, iValue = 0, iVersion = 0;

    int mod = 4;

    char* myheap = NULL;

	for(int i=0; i<TIMES; i++)
	{
        iKey = i;//random(1000000); 
        iValue = i;//random(100000000); 
        iVersion = i+1;//random(100000000)+1; 


		char csKey[16] = {0}, csValue[16] = {0};

		snprintf(csKey, sizeof(csKey), "%d", iKey);
		snprintf(csValue, sizeof(csValue), "%d", iValue);

		org_value_item_t valueItem;
		memset((void*)&valueItem, 0, sizeof(valueItem));
		valueItem.data_len_ = strlen(csValue);
		valueItem.data_ = csValue;
		valueItem.version_ = iVersion;
		int ret = org_set(testContext, cArea, csKey, strlen(csKey), 0, 0, 0, &valueItem, 1);
	}

	END_FUNC;

	return (void *)0;
}

void* testGetString(void* no)
{
	BEGIN_FUNC;

	uint32_t cArea = *((int*)no);

	printf("%s %u %d begin...\n", __func__, cArea, TIMES);
	int iKey = 0, iValue = 0, iVersion = 0;

    int mod = 4;

    char* myheap = NULL;

    iKey = random(100000); 

    char csKey[16] = {0}, csValue[16] = {0};

    snprintf(csKey, sizeof(csKey), "%u", iKey);

    org_value_item_t item;


	for(int i=0; i<TIMES; i++)
	{
		int ret = org_get(testContext, cArea, csKey, strlen(csKey), &item);
	}

	END_FUNC;

    float avg = cost_time * 1.0 / TIMES;
    printf("avg %0.3f\n", avg);

	return (void *)0;
}

static unsigned long long* gArr;

void init_global_array() {
    gArr = (unsigned long long*)lmalloc(1024*sizeof(unsigned long long));
}

void* testStatic(void* no)
{
    unsigned long long* lArr = (unsigned long long*)lmalloc(1024*sizeof(unsigned long long));
	BEGIN_FUNC;
 
    for(int j=0; j<TIMES; j+=1) {
        int sum = 0;
        if (j % 1000 == 0)
        { 
            BEGIN_SECTION;
        for(int i=0; i<100; i+=1) {
            sum += i;
        }
            END_SECTION;
        } else {
        for(int i=0; i<100; i+=1) {
            sum += i;
        }
        }

        uint32_t cArea = *((int*)no);
        if (j % 1000 == 0)
        {
            BEGIN_SECTION;
            gArr[cArea] += sum;
            gArr[cArea] -= sum;
            gArr[cArea] += sum;
            gArr[cArea] -= sum;
            gArr[cArea] += sum;
            gArr[cArea] -= sum;
            END_SECTION;
            if (cost_time>1)
                printf("global\n");
        } else {
            gArr[cArea] += sum;
            gArr[cArea] -= sum;
            gArr[cArea] += sum;
            gArr[cArea] -= sum;
            gArr[cArea] += sum;
            gArr[cArea] -= sum;
        }

        if (j % 1000 == 0)
        { 
            BEGIN_SECTION;
            lArr[cArea] += sum;
            lArr[cArea] -= sum;
            lArr[cArea] += sum;
            lArr[cArea] -= sum;
            lArr[cArea] += sum;
            lArr[cArea] -= sum;
            END_SECTION;
            if (cost_time>1)
                printf("local\n");
        } else {
            lArr[cArea] += sum;
            lArr[cArea] -= sum;
            lArr[cArea] += sum;
            lArr[cArea] -= sum;
            lArr[cArea] += sum;
            lArr[cArea] -= sum;
        }
    }

	END_FUNC;

    float avg = cost_time * 1.0 / TIMES;
    printf("avg %0.3f\n", avg);

	return (void *)0;
}

int testLocal(int cArea) {
    unsigned long long* lArr = (unsigned long long*)lmalloc(1024*sizeof(unsigned long long));

    BEGIN_FUNC;

    for(int i=0; i<10000; i+=1) {
        lArr[cArea] += i;
        lArr[cArea] -= i;
    }
    printf("testLocal\n");
    END_FUNC;

}

int testGlobal(int cArea) {
    BEGIN_FUNC;

    for(int i=0; i<10000; i+=1) {
        gArr[cArea] += i;
        gArr[cArea] -= i;
    }
    printf("testGlobal\n");
    END_FUNC;

}

int testGlobal2(void* ptr) {
    BEGIN_FUNC;

    int cArea = *(int*)(ptr);
    for(int i=0; i<10000; i+=1) {
        gArr[cArea] += i;
        gArr[cArea] -= i;
    }
    printf("testGlobal\n");
    END_FUNC;

}

int testMultiSetString(char* argv1, char* argv2)
{
	BEGIN_FUNC;

	NUM = atoi(argv1);
    TIMES = atoi(argv2);

	srand((int)time(0));

	int ids[NUM];
	pthread_t threads[NUM];

	for(int i=0; i<NUM; ++i)
	{   
		pthread_t tid;
		ids[i] = i;
		int error=pthread_create(&tid,NULL,testSetString,(void *)(&ids[i]));
		threads[i] = tid;
		printf("%s tid %llu\n", __func__, tid);
	}

	for(int i=0; i<NUM; ++i)
	{   
		pthread_join(threads[i], NULL);
	}

    END_FUNC;

    long long total = NUM * TIMES;

    float tps = total * 1000000 / cost_time;

    printf("%s total request %llu, tps: %0.3f per seconds\n", __func__, total, tps);

	return 0;
}

int testMultiGetString(char* argv1, char* argv2)
{
	BEGIN_FUNC;

	NUM = atoi(argv1);
    TIMES = atoi(argv2);

	srand((int)time(0));

	int ids[NUM];
	pthread_t threads[NUM];

	for(int i=0; i<NUM; ++i)
	{   
		pthread_t tid;
		ids[i] = i;
		int error=pthread_create(&tid,NULL,testGetString,(void *)(&ids[i]));
		threads[i] = tid;
		printf("%s tid %llu\n", __func__, tid);
	}

	for(int i=0; i<NUM; ++i)
	{   
		pthread_join(threads[i], NULL);
	}

    END_FUNC;

    long long total = NUM * TIMES;

    float tps = total * 1000000 / cost_time;

    printf("%s total request %llu, tps: %0.3f per seconds\n", __func__, total, tps);

	return 0;
}

int testSoloSetString(char* argv1, char* argv2)
{
	BEGIN_FUNC;

	NUM = atoi(argv1);
    TIMES = atoi(argv2);

	srand((int)time(0));

	int id = 1;

	for(int i=0; i<NUM; ++i)
	{   
		testSetString(&id);
	}


    END_FUNC;

    long long total = NUM * TIMES;

    float tps = total * 1000000 / cost_time;

    printf("%s total request %llu, tps: %0.3f per seconds\n", __func__, total, tps);

	return 0;
}

int testInit(const char* name)
{
	//BEGIN_FUNC;

    if(name != NULL){
        testContext = org_context_create(name, 2048, 1024);
    }else{
        testContext = org_context_create("/tmp/testdb_org", 2048, 1024);
    }
    if(testContext==NULL){
      printf("create ldb context failed, exit!\n");
      exit(1);
    }

	//END_FUNC;
    
    init_global_array();
	return 0;
}   

int main(int argc, char* argv[]){

    if (argc < 3 )
    {
        printf("<thread no> <calls per thread>\n");
        exit(0);
    }

    if( argc >= 4){
        testInit(argv[3]);
    }else{
	    testInit(NULL);
    }

	
	testMultiGetString(argv[1], argv[2]);

    testMultiSetString(argv[1], argv[2]);

    //testSoloSetString(argv[1], argv[2]);


    //int area = 1;
    //testSetString(&area);

	return 0;
}
