#include "ldb_session.h"
#include "ldb_context.h"
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

ldb_context_t *testContext = NULL;




#define BEGIN_FUNC  \
	struct timeval begin_tv; \
	gettimeofday(&begin_tv, NULL);

#define END_FUNC  \
	struct timeval end_tv; \
	gettimeofday(&end_tv, NULL); \
	unsigned cost_time = (end_tv.tv_sec - begin_tv.tv_sec) * 1000000; \
	cost_time += end_tv.tv_usec - begin_tv.tv_usec; \
	printf("%s begin %u.%u end %u.%u cost %u us\n", __func__, begin_tv.tv_sec, begin_tv.tv_usec, end_tv.tv_sec, end_tv.tv_usec, cost_time);

#define BEGIN_SECTION  \
	struct timeval begin_sec; \
	gettimeofday(&begin_sec, NULL);

#define END_SECTION \
	struct timeval end_sec; \
	gettimeofday(&end_sec, NULL); \
	unsigned cost_time = (end_sec.tv_sec - begin_sec.tv_sec) * 1000000; \
	cost_time += end_sec.tv_usec - begin_sec.tv_usec; \
    if (cost_time >= 1) \
	printf("%s SEC begin %u.%u end %u.%u cost %u us\n", __func__, begin_sec.tv_sec, begin_sec.tv_usec, end_sec.tv_sec, end_sec.tv_usec, cost_time);


#define random(x) (rand()%x)

int TIMES = 2000000;
int NUM = 0;

int arr1[1024] = {0};

int arr2[1024] = {0};

int arr3[1024] = {0};

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

		snprintf(csKey, sizeof(csKey), "%u", iKey);
		snprintf(csValue, sizeof(csValue), "%u", iValue);

		value_item_t valueItem;
		memset((void*)&valueItem, 0, sizeof(valueItem));
		valueItem.data_len_ = strlen(csValue);
		valueItem.data_ = csValue;
		valueItem.version_ = iVersion;
		int ret = ldb_set(testContext, cArea, csKey, strlen(csKey), 0, 0, 0, &valueItem, 1);
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

    value_item_t item;


	for(int i=0; i<TIMES; i++)
	{
		int ret = ldb_get(testContext, cArea, csKey, strlen(csKey), &item);
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

    printf("%s total request %lld, tps: %0.3f per seconds\n", __func__, total, tps);

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

    printf("%s total request %lld, tps: %0.3f per seconds\n", __func__, total, tps);

	return 0;
}

int testInit()
{
	//BEGIN_FUNC;

    testContext = ldb_context_create("/tmp/testdb", 128, 64);
    if(testContext==NULL){
      printf("create ldb context failed, exit!\n");
      exit(1);
    }

	//END_FUNC;
    
    //init_global_array();
	return 0;
}   

int main(int argc, char* argv[]){

    if (argc < 3 )
    {
        printf("<thread no> <calls per thread>\n");
        exit(0);
    }

	testInit();

	//testMultiSetString(argv[1], argv[2]);
	
	//testMultiGetString(argv[1], argv[2]);

	return 0;
}
