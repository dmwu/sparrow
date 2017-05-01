#define _GNU_SOURCE
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h> /* memset */
#include <unistd.h> /* close */
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
int main(int charc,char*args[]){
    clock_t start = clock();
    int iterations = 100;
    double ret =0;
    char fileName[30];
    char* memAlloc;
    int pid = getpid();
    sprintf(fileName,"%d.txt",pid);
    int fd = open(fileName,O_CREAT|O_RDWR, 0777);
    if(fd < 0){
        perror("opening file\n");
    }
    srand((unsigned int)time(NULL));
    double durSecs = strtod(args[1],NULL)/1000.0;// durations are specified in millis
    long long memCapacity = 100L*1024*1024*1024;
    double memSize = memCapacity*strtod(args[2],NULL); //assuming the maximum memory is 100GB
    double ioTime = strtod(args[3],NULL);
    if((int)memSize>0){
         memAlloc= (char*) malloc((int)memSize*sizeof(char));
    }
    if(memAlloc!=NULL){
        memset(memAlloc, 0, memSize);
    }
    int blockSize = 1*1024;

    char *data = (char*)calloc(blockSize, sizeof(char));

    double accu_ioTime = 0.01;
    double ioelapsed = 0.0;

    clock_t end = clock();
    struct timeval st,ed;

    while(((double) (end - start) / CLOCKS_PER_SEC) < durSecs){
        int j =0;
        for(j =0; j < iterations; j++){
            double x = rand() / (double)RAND_MAX;
            ret = ret*x;
        }

        if(accu_ioTime < ioTime){
            gettimeofday(&st,NULL);
            if(write(fd, data, blockSize) <0 ){
                perror("writing to file\n");
            }
            gettimeofday(&ed,NULL);
            ioelapsed = (ed.tv_sec-st.tv_sec)*1000.0 + (ed.tv_usec-st.tv_usec)/1000.0;
            // printf("elapsed time %f \n",ioelapsed);
            accu_ioTime += ioelapsed;
        }
        end = clock();
    }
    //printf("cpu time: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
    //printf("io time: %f\n", accu_ioTime);
    if(remove(fileName) !=0){
        perror("delete file\n");
    }
    return (int)ret;
}