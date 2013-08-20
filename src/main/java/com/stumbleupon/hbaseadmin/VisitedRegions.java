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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of regions we have visited and when we last visited it
 * Provides functions for maintaining structure
 */
public class VisitedRegions {
    
    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(VisitedRegions.class);
    
    /** A map of visited regions per host, each region contains the timestamp of when it was last visited */
    private Map<String, Map<String, Long>> regionsPerHost = new HashMap<String, Map<String, Long>>();
    
    /** The time to remember how long a region was last visited */
    private long timeToRememberVisit = Long.MAX_VALUE;

    /**
     * Construct a new VisitedRegions
     * @param ttl the time to remember how long a region was last visited. A value of -1 means
     * visited regions will be immediately forgotten when maintainVisitedRegions() is called
     */
    public VisitedRegions(long ttl) {
        this.timeToRememberVisit = ttl;
    }
    
    /**
     * Mark a region as visited and assign a timestamp
     * @param hostport The region server that hosts the region
     * @param regionName The region
     */
    public void visitRegion(String hostport, String regionName) {
        Map<String, Long> regions = regionsPerHost.get(hostport);
        if (regions == null) {
            regions = new HashMap<String, Long>();
            regionsPerHost.put(hostport, regions);
        }
        
        LOG.debug("Visiting {} [{}]", hostport, regionName);
        regions.put(regionName, System.currentTimeMillis());
    }
    
    /**
     * Determine if region has been visited
     * @param hostport the region server hosting the region
     * @param regionName the region
     * @return True if the region has been visited, false otherwise
     */
    public boolean isRegionVisited(String hostport, String regionName) {
        boolean ret = false;
        Map<String, Long> regions = regionsPerHost.get(hostport);
        
        if (regions != null) {
            Long timeVisited = regions.get(regionName);
            
            if (timeVisited != null) {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * Maintain the internal data structure
     */
    public void maintainVisitedRegions() {
        if (this.timeToRememberVisit < 0) {
            regionsPerHost.clear();
        } else {
            long now = System.currentTimeMillis();
            Iterator<Entry<String, Map<String, Long>>> itRegionsPerHost = regionsPerHost.entrySet().iterator();
            
            while (itRegionsPerHost.hasNext()) {
                Entry<String, Map<String, Long>> regionsPerHostEntry = itRegionsPerHost.next();
                String hostport = regionsPerHostEntry.getKey();
                Map<String, Long> regions = regionsPerHostEntry.getValue();
                
                Iterator<Entry<String, Long>> itRegions = regions.entrySet().iterator();
                while (itRegions.hasNext()) {
                    Entry<String, Long> regionEntry = itRegions.next();
                    String regionName = regionEntry.getKey();
                    Long timeVisited  = regionEntry.getValue();
                    long timeRemembered = now - timeVisited;
                    
                    if (timeRemembered > this.timeToRememberVisit) {
                        LOG.debug("Removing region '{}' from list of visited regions", regionName);
                        itRegions.remove();
                    }
                }
                
                if (regions.size() == 0) {
                    LOG.debug("Removing host '{}'. Contains no regions", hostport);
                    itRegionsPerHost.remove();
                }
            }
        }
    }
}
