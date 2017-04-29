/*
 * Copyright 2013 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.berkeley.sparrow.daemon.util.Network;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Simple frontend that runs jobs composed of sleep tasks.
 */
public class SimpleFrontend implements FrontendService.Iface {
  /** Amount of time to launch tasks for. */
  public static final String EXPERIMENT_S = "experiment_s";
  public static final int DEFAULT_EXPERIMENT_S = 30;

  public static final String JOB_ARRIVAL_PERIOD_MILLIS = "job_arrival_period_millis";
  public static final int DEFAULT_JOB_ARRIVAL_PERIOD_MILLIS = 100;

  /** Number of tasks per job. */
  public static final String TASKS_PER_JOB = "tasks_per_job";
  public static final int DEFAULT_TASKS_PER_JOB = 1;

  /** Duration of one task, in milliseconds */
  public static final String TASK_DURATION_MILLIS = "task_duration_millis";
  public static final int DEFAULT_TASK_DURATION_MILLIS = 100;

  /** Host and port where scheduler is running. */
  public static final String SCHEDULER_HOST = "scheduler_host";
  public static final String DEFAULT_SCHEDULER_HOST = "localhost";
  public static final String SCHEDULER_PORT = "scheduler_port";
  public static final String TRACE_FILE_NAME = "trace_file";
  /**
   * Default application name.
   */
  public static final String APPLICATION_ID = "simpleApp";

  private static final Logger LOG = Logger.getLogger(SimpleFrontend.class);

  private static final TUserGroupInfo USER = new TUserGroupInfo();

  private SparrowFrontendClient client;
  private double traceCPUmean;
  private int traceJSmean;
  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private class JobLaunchRunnable implements Runnable {
    private int tasksPerJob;
    private int taskDurationMillis;

    public JobLaunchRunnable(int tasksPerJob, int taskDurationMillis) {
      this.tasksPerJob = tasksPerJob;
      this.taskDurationMillis = taskDurationMillis;
    }

    @Override
    public void run() {
      // Generate tasks in the format expected by Sparrow. First, pack task parameters.
      ByteBuffer message = ByteBuffer.allocate(4);
      message.putInt(taskDurationMillis);

      List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
      for (int taskId = 0; taskId < tasksPerJob; taskId++) {
        TTaskSpec spec = new TTaskSpec();
        spec.setTaskId(Integer.toString(taskId));
        spec.setMessage(message.array());
        tasks.add(spec);
      }
      long start = System.currentTimeMillis();
      try {
        client.submitJob(APPLICATION_ID, tasks, USER);
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
      long end = System.currentTimeMillis();
      LOG.debug("Scheduling request duration " + (end - start));
    }
  }

  public void run(String[] args) {
    try {
      OptionParser parser = new OptionParser();
      parser.accepts("c", "configuration file").withRequiredArg().ofType(String.class);
      parser.accepts("help", "print help statement");
      OptionSet options = parser.parse(args);

      if (options.has("help")) {
        parser.printHelpOn(System.out);
        System.exit(-1);
      }

      // Logger configuration: log to the console
      BasicConfigurator.configure();
      LOG.setLevel(Level.DEBUG);

      PropertyConfigurator.configure("src/log4j.properties");
      Configuration conf = new PropertiesConfiguration();

      if (options.has("c")) {
        String configFile = (String) options.valueOf("c");
        conf = new PropertiesConfiguration(configFile);
      }
      int schedulerPort = conf.getInt(SCHEDULER_PORT,
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
      //need to randomly pick a scheduler if it is not on localhost
      String schedulerIp = Network.getIPAddress(conf);
      client = new SparrowFrontendClient();
      client.initialize(new InetSocketAddress(schedulerIp, schedulerPort), APPLICATION_ID, this);
      launchTasks(conf);
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }

  void launchTasks(Configuration conf){
    String traceFile = conf.getString(TRACE_FILE_NAME,"trace_file");
    int machineNum = conf.getInt("machine_num",24);
    int coresPerMachine = conf.getInt("cores_per_machine",6);
    double load = conf.getDouble("load",0.75);
    double avgInterArrivalDelay = traceCPUmean*traceJSmean/load;
    long nextLaunchTime =0, lastLaunchTime =System.currentTimeMillis();
    List<JobSpec> jobTrace = parseJobsFromFile(traceFile);

    for (JobSpec job : jobTrace) {
      JobLaunchRunnable runnable = new JobLaunchRunnable(job.numTasks, job.durationMilli);
      runnable.run();
      nextLaunchTime = lastLaunchTime + (long) avgInterArrivalDelay;
      lastLaunchTime = nextLaunchTime;
      long toWait = Math.max(0, nextLaunchTime - System.currentTimeMillis());
      if (toWait == 0) {
        //[WDM] means that the current task launch is lagging behind.
        //LOG.warn("Launching task after start time in generated workload.");
      } else {
        try {

          Thread.sleep(toWait);
        } catch (InterruptedException e) {
          LOG.error("Interrupted sleep: " + e.getMessage());
          return;
        }
      }
    }
  }

  @Override
  public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
      throws TException {
    // We don't use messages here, so just log it.
    LOG.debug("Got unexpected message: " + Serialization.getByteBufferContents(message));
  }
  class JobSpec{
    int durationMilli;
    int ioMilli;
    double memRatio;
    int numTasks;
    JobSpec(int duration, double mem, int io, int js){
      durationMilli = duration;
      ioMilli = io;
      memRatio = mem;
      numTasks = js;
    }
  }

  private List<JobSpec> parseJobsFromFile(String file){
    BufferedReader br;
    String sCurrentLine;
    double maxAllowedTaskDurationInMillis = 3.6e6;
    double sumLoad = 0;
    int sumJs = 0;
    List<JobSpec> jobTrace = new ArrayList<JobSpec>();
    try {
      br = new BufferedReader(new FileReader(file));
      while ((sCurrentLine = br.readLine()) != null) {
          String[] items = sCurrentLine.split(" ");
          int jobsize = Integer.parseInt(items[4]);
          //scaling down by 1000x, original data are in seconds, now are in millis
          double cpu = Double.parseDouble(items[1]);
          double mem = Double.parseDouble(items[2])/jobsize;
          double io = Double.parseDouble(items[3]);
          int scaledJs = Math.max(jobsize/166,1);
          int taskCPU = (int)(cpu/scaledJs);
          int taskIO = (int)(io/scaledJs);
          if(taskCPU >0 || taskCPU<=maxAllowedTaskDurationInMillis){
            jobTrace.add(new JobSpec(taskCPU, mem, taskIO, scaledJs));
            sumLoad += taskCPU*scaledJs;
            sumJs+=scaledJs;
          }
        }

    } catch (IOException e) {
      e.printStackTrace();
    }
    traceCPUmean = sumLoad/sumJs;
    traceJSmean = sumJs/jobTrace.size();
    LOG.info("num of jobs: "+jobTrace.size()+" traceCPUmean: "+traceCPUmean);
    return jobTrace;
  }

  public static void main(String[] args) {
    new SimpleFrontend().run(args);
  }
}
