package edu.berkeley.sparrow.daemon.scheduler;

import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wdm on 4/30/17.
 */
public class JobTracker {
        private static final Logger LOG = Logger.getLogger(JobTracker.class);
        private String appId;
        private int jobId;
        private int jobSize;
        private AtomicInteger numFinishedTasks;
        JobTracker(String appId, int jobId,int jobSize){
                this.appId = appId;
                this.jobId = jobId;
                this.jobSize = jobSize;
                this.numFinishedTasks = new AtomicInteger(0);
        }

        void oneTaskFinish(){
                this.numFinishedTasks.incrementAndGet();
        }

        boolean allTasksFinished(){
                return numFinishedTasks.get() >= jobSize;
        }

}
