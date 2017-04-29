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

package edu.berkeley.sparrow.daemon;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Logging;
import org.apache.log4j.PropertyConfigurator;

/**
 * A Sparrow Daemon includes both a scheduler and a node monitor.
 * The main() method launches a Sparrow daemon.
 */
public class SparrowDaemon {
  // Eventually, we'll want to change this to something higher than debug.
  public final static Level DEFAULT_LOG_LEVEL = Level.DEBUG;

  public final static Logger LOG = Logger.getLogger(SparrowDaemon.class);


  public void initialize(Configuration conf,String component) throws Exception {
    Level logLevel = Level.toLevel(conf.getString(SparrowConf.LOG_LEVEL, ""),
        DEFAULT_LOG_LEVEL);
    PropertyConfigurator.configure("src/log4j.properties");
    if(component.toLowerCase().equals("scheduler")){
      LOG.info("Starting SparrowDaemon Scheduler");
      SchedulerThrift scheduler = new SchedulerThrift();
      scheduler.initialize(conf);

    }else if(component.toLowerCase().equals("nodemonitor")) {
        // Start as many node monitors as specified in config
        LOG.info("Start SparrowDaemon NodeMonitor");
        String[] nmPorts = conf.getStringArray(SparrowConf.NM_THRIFT_PORTS);
        String[] inPorts = conf.getStringArray(SparrowConf.INTERNAL_THRIFT_PORTS);

        if (nmPorts.length != inPorts.length) {
          throw new ConfigurationException(SparrowConf.NM_THRIFT_PORTS + " and " +
                  SparrowConf.INTERNAL_THRIFT_PORTS + " not of equal length");
        }
        if (nmPorts.length > 1 &&
                (!conf.getString(SparrowConf.DEPLYOMENT_MODE, "").equals("standalone"))) {
          throw new ConfigurationException("Mutliple NodeMonitors only allowed " +
                  "in standalone deployment");
        }
        if (nmPorts.length == 0) {
          (new NodeMonitorThrift()).initialize(conf,
                  NodeMonitorThrift.DEFAULT_NM_THRIFT_PORT,
                  NodeMonitorThrift.DEFAULT_INTERNAL_THRIFT_PORT);
        } else {
          for (int i = 0; i < nmPorts.length; i++) {
            (new NodeMonitorThrift()).initialize(conf,
                    Integer.parseInt(nmPorts[i]), Integer.parseInt(inPorts[i]));
          }
        }
    }else{
      LOG.error("illegal deamon component!");
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    parser.accepts("c", "configuration file (required)").
      withRequiredArg().ofType(String.class);
    parser.accepts("t", "deamon component,either scheduler or nodemonitor (required)").
            withRequiredArg().ofType(String.class);
    parser.accepts("help", "print help statement");
    OptionSet options = parser.parse(args);

    if (options.has("help") || !options.has("c") || !options.has("t")) {
      parser.printHelpOn(System.out);
      System.exit(-1);
    }
    LOG.setLevel(DEFAULT_LOG_LEVEL);
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();

    Logging.configureAuditLogging();

    String configFile = (String) options.valueOf("c");
    String component = (String) options.valueOf("t");
    Configuration conf = new PropertiesConfiguration(configFile);
    SparrowDaemon sparrowDaemon = new SparrowDaemon();
    if(args.length<1){
      LOG.error("Not enough paremeter!");
    }
    sparrowDaemon.initialize(conf,component);
  }
}
