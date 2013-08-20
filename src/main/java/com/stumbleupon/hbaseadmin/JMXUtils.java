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
 * Contains pre-defined functions for querying a jmx server
 */
public final class JMXUtils {
    
    /**
     * Cannot instantiate
     */
    private JMXUtils() { }
    
    /**
     * Get the compaction queue size
     * @param spec the jmx server specs
     * @return The compaction size
     * @throws NumberFormatException If returned value is not a number
     * @throws Exception 
     */
    public static int getCompactionQueueSize(JmxServerSpec spec)
        throws NumberFormatException, Exception {
        return queryJMXIntValue(spec, "hadoop:name=RegionServerStatistics,service=RegionServer", "compactionQueueSize");
    }
    
    /**
     * Get the number of available processors
     * @param spec the jmx server specs
     * @return The compaction size
     * @throws NumberFormatException If returned value is not a number
     * @throws Exception 
     */
    public static int getAvailableProcessors(JmxServerSpec spec)
        throws NumberFormatException, Exception {
        return queryJMXIntValue(spec, "java.lang:type=OperatingSystem", "AvailableProcessors");
    }
    
    /**
     * Get a jmx integer value from jmx server
     * @param spec the jmx server specs
     * @param mbean The bean
     * @param command The command
     * @return The jmx value
     * @throws NumberFormatException  
     * @throws Exception  
     */
    public static int queryJMXIntValue(JmxServerSpec spec, String mbean, String command)
        throws NumberFormatException, Exception {
        final JMXQuery client = new JMXQuery(mbean, command);
        return (new Integer(client.execute(spec))).intValue();
    }
}
