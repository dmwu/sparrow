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

package edu.berkeley.sparrow.daemon.util;

import java.net.*;
import java.util.Enumeration;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.thrift.THostPort;

import static edu.berkeley.sparrow.daemon.SparrowDaemon.LOG;

public class Network {
  
  public static THostPort socketAddressToThrift(InetSocketAddress address) {
    return new THostPort(address.getAddress().getHostAddress(), address.getPort());
  }

  /** Return the hostname of this machine, based on configured value, or system
   * Interrogation. */
  public static String getHostName(Configuration conf) {
    String defaultHostname = null;
    try {
      defaultHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      defaultHostname = "localhost";
    }
    return conf.getString(SparrowConf.HOSTNAME, defaultHostname); 
  }
  
  /**
   * Return the IP address of this machine, as determined from the hostname
   * specified in configuration or from querying the machine.
   */
  public static String getIPAddress(Configuration conf) {
    String nicName =  conf.getString("nic_name","eth2");
    String ip = getIPAddressByNICName(nicName);
    return ip;
  }

  public static String getIPAddressByNICName(String nicName) {
    try {
       NetworkInterface n = NetworkInterface.getByName(nicName);
       Enumeration ee = n.getInetAddresses();
       while(ee.hasMoreElements()) {
         InetAddress i = (InetAddress) ee.nextElement();
         if(i instanceof Inet4Address)
            return i.getHostAddress();
       }
      }
      catch (SocketException e) {
        return "IP UNKONWN";
      }
      return "IP UNKNOWN";
  }
}
