/*
 * Copyright (c) 2013
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stumbleupon.hbaseadmin;

/**
 * JmxSpecs for a jmx query
 */
public class JmxServerSpec {
    /** the host name */
    private String host;
    
    /** the jmx port */
    private String port;
    
    /** the password file, can be null */
    private String passwordFile;
    
    /** The host port */
    private String hostport;

    /**
     * Construct a new JmxSpec
     * @param host The host name
     * @param port The jmx port
     * @param passwordFile The jmx password file
     */
    public JmxServerSpec(String host, String port, String passwordFile) {
        set(host, port, passwordFile);
    }
    
    /**
     * Set the JmxSpec
     * @param host The host name
     * @param port The jmx port
     * @param passwordFile The jmx password file
     */
    public void set(String host, String port, String passwordFile) {
        this.host = host;
        this.port = port;
        this.hostport = host + ":" + port;
    }
    
    /**
     * Get the host
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the jmx port
     * @return the port
     */
    public String getPort() {
        return port;
    }

    /**
     * Get the jmx password file
     * @return the passwordFile
     */
    public String getPasswordFile() {
        return passwordFile;
    }

    /**
     * Get the host port
     * @return the hostport
     */
    public String getHostport() {
        return hostport;
    }
}
