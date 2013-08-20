/**
 * This file is part of hbaseadmin.
 * Copyright (C) 2011 StumbleUpon, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. This program is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy
 * of the GNU Lesser General Public License along with this program. If not,
 * see <http: *www.gnu.org/licenses/>.
 */

package com.stumbleupon.hbaseadmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state of the cluster.
 */
class ClusterUtils {

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(ClusterUtils.class);

    /** tracker sleep */
    private static final int TRACKER_SLEEP = 100;
    
    /** count comparator? */
    private Comparator<String> rsComparator = new ClusterUtils.RSCountCompare();
    
    /** server -> regions map for regions yet to be compacted. */
    private HashMap<String, List<HRegionInfo>> serverRegionMap = new HashMap<String, List<HRegionInfo>>();

    /** server -> cpu_count map for servers. */
    private HashMap<String, Integer> serverCpuMap = new HashMap<String, Integer>();

    /** server list? */
    private HashMap<String, ServerName> serverNameMap = new HashMap<String, ServerName>();
    
    /** map of HRegionInterfaces keyed by hostport */
    private HashMap<String, HRegionInterface> serverInterfaceMap = new HashMap<String, HRegionInterface>();

    /** map of HRegionInterfaces keyed by hostport */
    private HashMap<String, HRegionInfo> regionInfoMap = new HashMap<String, HRegionInfo>();
    
    /** map of hostport keyed by region name */
    private HashMap<String, String> regionHostPortMap = new HashMap<String, String>();
    
    /** map of htable descriptors */
    private HashMap<String, HTableDescriptor> tableDescriptorMap = new HashMap<String, HTableDescriptor>();
    
    /** Data structure containing regions that have been visited */
    private VisitedRegions visitedRegions = null;
    
    /** The cluster status */
    private ClusterStatus clusterStatus = null;

    /** hbase compact tool */
    private HBaseCompact compact = null;
    
    /** The jmx port across the entire cluster */
    private String jmxPort = "9021";
    
    /** the password file across the entire cluster */
    private String jmxPasswordFile = null;

    /**
     * Construct a new ClusterUtils
     * @param compact The HBaseCompact object
     * @param timeToRememberCompactions The max time to remember when a region was last compacted
     */
    public ClusterUtils(HBaseCompact compact, long timeToRememberCompactions) {
        this.visitedRegions = new VisitedRegions(timeToRememberCompactions);
        this.compact = compact;
    }

    /**
     * returns the next region to be processed and removes it from the list of regions for the server. When the server
     * has no more regions, it's removed from the sregions map. sregions: map of server names and a list of regions on
     * the server.
     * @param regionName The region name
     * @param throttleFactor The throttle factor
     * @return The next region to process, or null if none found.
     */
    public HRegionInfo getNextRegion(final String regionName, final int throttleFactor) {
        HRegionInfo ret = null;
        
        if (regionInfoMap.containsKey(regionName)) {
            String hostport = regionHostPortMap.get(regionName);
            String host = hostport.split(":")[0];
            JmxServerSpec spec = new JmxServerSpec(host, jmxPort, jmxPasswordFile);
            
            try {
                final int compactionQueueSize = JMXUtils.getCompactionQueueSize(spec);
                if (compactionQueueSize < (serverCpuMap.get(hostport).intValue() / throttleFactor)) {
                    ret = regionInfoMap.get(regionName);
                    visitedRegions.visitRegion(hostport, ret.getRegionNameAsString());
                } else {
                    LOG.info("Region server {} has a queue size of {}, skipping host for this round.",
                        hostport, compactionQueueSize);
                }
            } catch (Exception e) {
                LOG.warn("Could not get next region for server {}", hostport, e);
            }
        }
        
        return ret;
    }
    
    /**
     * Updates information on previously visited regions
     */
    public void updateForNextRun() {
        visitedRegions.maintainVisitedRegions();
        tableDescriptorMap.clear();
    }

    /**
     * Get ServerName for given hostport
     * @param hostport The host port
     * @return The ServerName object. Null if not found
     */
    public ServerName getServerName(String hostport) {
        ServerName ret = null;
        
        if (serverNameMap.containsKey(hostport)) {
            ret = serverNameMap.get(hostport);
        }
        
        return ret;
    }
    
    /**
     * Get the cluster status as of the last cluster update
     * @return The most recent cluster status
     */
    public ClusterStatus getClusterStatus() {
        return this.clusterStatus;
    }

    /**
     * Get the next set of region names
     * @param admin The hbase admin
     * @return The next set of region names
     * @throws IOException 
     */
    public Set<String> getRegionNames(HBaseAdmin admin) throws IOException {
        LOG.debug("Getting next set of region names");
        clusterStatus = admin.getClusterStatus();
        HConnection connection = admin.getConnection();
        Collection<ServerName> servers = clusterStatus.getServers();
        Collection<ServerName> deadServers = clusterStatus.getDeadServerNames();
        
        serverNameMap.clear();
        serverCpuMap.clear();
        regionInfoMap.clear();
        regionHostPortMap.clear();
        serverInterfaceMap.clear();
        
        for (ServerName si : servers) {
            try {
                if (!deadServers.contains(si)) {
                    final String hostport = si.getHostAndPort();
                    LOG.debug("Getting online regions from: {}", hostport);

                    // Get a fresh list of regions from this region server                   
                    final HRegionInterface server = connection.getHRegionConnection(si.getHostname(), si.getPort());
                    HRegionInfo onlineRegion = getNextEligibleRegion(admin, si, server);

                    // When we visit all the regions in a server, then this list is empty
                    if (onlineRegion != null) {
                        JmxServerSpec jmxServerSpec = new JmxServerSpec(si.getHostname(), jmxPort, jmxPasswordFile);
                        int procs = JMXUtils.getAvailableProcessors(jmxServerSpec);
                        
                        serverNameMap.put(hostport, si);
                        regionInfoMap.put(onlineRegion.getRegionNameAsString(), onlineRegion);
                        regionHostPortMap.put(onlineRegion.getRegionNameAsString(), hostport);
                        serverCpuMap.put(hostport, new Integer(procs));
                        serverInterfaceMap.put(hostport, server);
                    }
                } else {
                    LOG.info("Skipping server {} because marked as dead", si.getHostAndPort());
                }
            } catch (RetriesExhaustedException e) {
                LOG.warn("Server down: {}", si, e);
            } catch (IOException e) {
                LOG.warn("Problem querying server: {}", si, e);
            } catch (Exception e) {
                LOG.warn("Could not get server status: {}", si, e);
            }
        }

        return regionInfoMap.keySet();
    }

    /**
     * Remove any regions that do not qualify for compaction
     * @param admin The hbase admin
     * @param serverName The server name
     * @param server The HRegion interface
     * @return The filtered regions
     * @throws IOException 
     */
    private HRegionInfo getNextEligibleRegion(HBaseAdmin admin, ServerName serverName, HRegionInterface server)
        throws IOException {
        HRegionInfo ret = null;
        List<HRegionInfo> onlineRegions = server.getOnlineRegions();
        String hostport = serverName.getHostAndPort();
        HServerLoad serverLoad = clusterStatus.getLoad(serverName);
        
        if (serverLoad == null) {
            LOG.warn("Skipping server {} because could not get server load", hostport);
        } else {
            List<String> tableNames = compact.getTableNames();
            boolean excludeFromList = compact.getExcludeTables();
            Map<byte[], RegionLoad> regionLoadMap = serverLoad.getRegionsLoad();
            List<String> reasons = new ArrayList<String>();

            for (HRegionInfo region : onlineRegions) {
                String regionName = region.getRegionNameAsString();
                String tableName = region.getTableNameAsString();
                reasons.clear();
                
                // Ignore any regions in tables that are marked as excluded
                if (tableNames.size() > 0) {
                    if (excludeFromList && tableNames.contains(tableName)) {
                        continue;
                    } else if (!excludeFromList && !tableNames.contains(tableName)) {
                        continue;
                    } else if (LOG.isDebugEnabled()) {
                        reasons.add(hostport + " [" + regionName + "] qualifies because its table '"
                            + tableName + "' has NOT been excluded");
                    }
                }
                
                // Ignore any regions that we have already visited/compacted
                if (visitedRegions.isRegionVisited(hostport, regionName)) {
                    continue;
                } else if (LOG.isDebugEnabled()) {
                    reasons.add(hostport + " [" + regionName + "] qualifies because it has NOT been visited");
                }

                // Remove any regions that do not have enough store files to qualify for compaction
                RegionLoad regionLoad = regionLoadMap.get(region.getRegionName());
                boolean isRegionEligible = true;
                
                if (regionLoad == null) {
                    LOG.warn("Could not get region load for '{}'. Skipping region...", regionName);
                    continue;
                } else {
                    try {
                        int numFamilies = getTableDescriptor(admin, region).getColumnFamilies().length;
                        int numRegionStoreFiles = regionLoad.getStorefiles();
                        int minStoreFilesNeeded = compact.getNumStoreFiles() * numFamilies;
                        
                        if (numRegionStoreFiles >= minStoreFilesNeeded) {
                            isRegionEligible = true;
                            
                            if (LOG.isDebugEnabled()) {
                                reasons.add(hostport + " [" + regionName + "] qualifies because it has a total of "
                                    + numRegionStoreFiles + " store files in " + numFamilies + " families");
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                reasons.add(hostport + " [" + regionName
                                    + "] does not qualify because it has a total of "
                                    + numRegionStoreFiles + " store files in " + numFamilies
                                    + " families. Needs at least " + minStoreFilesNeeded);
                            }

                            isRegionEligible = false;
                        }
                    } catch (TableNotFoundException e) {
                        LOG.error("Could not determine if region '{}' is eligible. Skipping region.", regionName, e);
                        continue;
                    } catch (IOException e) {
                        LOG.error("Could not determine if region '{}' is eligible. Skipping region.", regionName, e);
                        continue;
                    } catch (Exception e) {
                        LOG.error("Could not determine if region '{}' is eligible. Skipping region.", regionName, e);
                        continue;
                    }
                }
                
                // If enabled, force compaction of any regions that contain store files older than maxStoreFileAge 
                if (!isRegionEligible && compact.getMaxStoreFileAge() > 0) {
                    List<String> files = server.getStoreFileList(region.getRegionName());
                    FileSystem fs = FileSystem.get(admin.getConfiguration());
                    
                    if (files != null) {
                        Path[] filePaths = new Path[files.size()];
                        for (int i = 0; i < files.size(); i++) {
                            filePaths[i] = new Path(files.get(0));
                        }
                        
                        long maxStoreFileAge = compact.getMaxStoreFileAge();
                        long now = System.currentTimeMillis();
                        FileStatus[] storeFilesStatus = fs.listStatus(filePaths);
                        
                        for (FileStatus fileStatus : storeFilesStatus) {
                            long storeFileAge = now - fileStatus.getModificationTime();
                            
                            if (storeFileAge > maxStoreFileAge) {
                                isRegionEligible = true;

                                if (LOG.isDebugEnabled()) {
                                    reasons.add(hostport + " [" + regionName + "] forced to qualify because "
                                        + "at least one store file is older than the specified maxStoreFileAge");
                                }
                                
                                break;
                            }
                        }
                    }
                }

                if (isRegionEligible) {
                    if (reasons.size() > 0) {
                        for (String reason : reasons) {
                            LOG.debug(reason);
                        }
                    }

                    ret = region;
                    break;
                }
            }
        }

        return ret;
    }

    /**
     * Get the table descriptor for a given region and cache it.
     * @param admin The hbase admin
     * @param region The region
     * @return The table descriptor, null if not found
     * @throws TableNotFoundException 
     * @throws IOException 
     */
    private HTableDescriptor getTableDescriptor(HBaseAdmin admin, HRegionInfo region)
        throws TableNotFoundException, IOException {
        HTableDescriptor ret = null;
        String tableName = region.getTableNameAsString();
        
        // This cached version of getTableDescriptor increases performance significantly in clusters
        // with multiple hosts. Without it, it wastes a lot of time doing this call alone.
        if (tableDescriptorMap.containsKey(tableName)) {
            ret = tableDescriptorMap.get(tableName);
        } else {
            ret = admin.getTableDescriptor(region.getTableName());
            if (ret != null) {
                tableDescriptorMap.put(tableName, ret);
            }
        }
        
        
        return ret;
    }
    
    /**
     * Get the region server's HRegionInterface
     * @return the serverInterfaceMap
     */
    public HashMap<String, HRegionInterface> getServerInterfaceMap() {
        return serverInterfaceMap;
    }

    /**
     * Set the region server's HRegionInterface
     * @param serverInterfaceMap the serverInterfaceMap to set
     */
    public void setServerInterfaceMap(HashMap<String, HRegionInterface> serverInterfaceMap) {
        this.serverInterfaceMap = serverInterfaceMap;
    }

    /**
     * Get teh number of regions for a host
     * @param hostport The host port
     * @return The count, or -1 if host doesn't exist
     */
    private int getNumRegions(final String hostport) {
        int ret = -1;

        if (serverRegionMap.containsKey(hostport)) {
            ret = serverRegionMap.get(hostport).size();
        }

        return ret;
    }

    /**
     * Get the jmx port
     * @return the jmxPort
     */
    public String getJmxPort() {
        return jmxPort;
    }

    /**
     * Set the jmx port
     * @param jmxPort the jmxPort to set
     */
    public void setJmxPort(String jmxPort) {
        this.jmxPort = jmxPort;
    }

    /**
     * Get the path to the jmx password file
     * @return the jmxPasswordFile
     */
    public String getJmxPasswordFile() {
        return jmxPasswordFile;
    }

    /**
     * Set the path to the jmx password file
     * @param jmxPasswordFile the jmxPasswordFile to set
     */
    public void setJmxPasswordFile(String jmxPasswordFile) {
        this.jmxPasswordFile = jmxPasswordFile;
    }

    /**
     * Get the host port for a given region
     * @param regionName the region name
     * @return The host:port string
     */
    public String getRegionHostPort(String regionName) {
        return regionHostPortMap.get(regionName);
    }
    
    /**
     * Return an array of hostports sorted by their region count
     * @param sregions The hostport -> regions map
     * @return The sorted array
     */
    public String[] sortByRegionCount(final HashMap<String, List<HRegionInfo>> sregions) {
        String[] rsArray = sregions.keySet().toArray(new String[0]);
        Arrays.sort(rsArray, rsComparator);
        return rsArray;
    }

    /**
     * Get the regions for this server from a given map
     * @param hostport The host port
     * @param sregions The map to look in
     * @return The list of regions for the given host, or null if not found
     */
    public List<HRegionInfo> getRegionsOnServer(final String hostport,
        final HashMap<String, List<HRegionInfo>> sregions) {
        List<HRegionInfo> ret = null;

        if (sregions.containsKey(hostport)) {
            ret = sregions.get(hostport);
        }

        return ret;
    }

    /**
     * This is pretty much a java re-write of Stack's isSuccessfulScan
     * @param admin the HBaseAdmin to use
     * @param region The region to check
     * @return Tru if region is live. False otherwise
     * @throws Exception 
     */
    public boolean isRegionLive(final HBaseAdmin admin, final HRegionInfo region) throws Exception {
        final byte[] startKey = region.getStartKey();
        final Scan scan = new Scan(startKey);
        final byte[] tableName = HRegionInfo.getTableName(region.getRegionName());

        boolean ret = false;
        HTable htable = null;
        ResultScanner scanner = null;

        scan.setBatch(1);
        scan.setCaching(1);
        scan.setFilter(new FirstKeyOnlyFilter());

        try {
            htable = new HTable(admin.getConfiguration(), tableName);

            if (htable != null) {
                scanner = htable.getScanner(scan);

                if (scanner != null) {
                    scanner.next();
                    ret = true;
                }
            }

        } catch (IOException e) {
            // Nothing to do?
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            
            if (htable != null) {
                htable.close();
            }
        }

        return ret;
    }

    /**
     * This is pretty much a java re-write of Stack's getServerNameForRegion
     * @param admin Hbase admin
     * @param region the region info
     * @return The server hosting the given region
     * @throws Exception 
     */
    @SuppressWarnings("deprecation")
    public String getServerHostingRegion(final HBaseAdmin admin, final HRegionInfo region) throws Exception {
        String ret = null;
        ServerName server = null;
        HConnection connection = admin.getConnection();

        if (region.isRootRegion()) {
            final RootRegionTracker tracker = new RootRegionTracker(connection.getZooKeeperWatcher(), new Abortable() {
                private boolean aborted = false;

                @Override
                public void abort(String why, Throwable e) {
                    LOG.error("ZK problems: {}", why);
                    aborted = true;
                }

                @Override
                public boolean isAborted() {
                    return aborted;
                }
            });
            
            tracker.start();
            while (!tracker.isLocationAvailable()) {
                Thread.sleep(TRACKER_SLEEP);
            }
            
            server = tracker.getRootRegionLocation();
            tracker.stop();
            ret = (server.getHostname() + ":" + server.getPort());
        } else {
            HTable table;
            final Configuration conf = admin.getConfiguration();
    
            if (region.isMetaRegion()) {
                table = new HTable(conf, HConstants.ROOT_TABLE_NAME);
            } else {
                table = new HTable(conf, HConstants.META_TABLE_NAME);
            }
    
            Get get = new Get(region.getRegionName());
            get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    
            Result result = table.get(get);
            table.close();
    
            final byte[] servername = result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
            ret = Bytes.toString(servername);
        }
        
        return ret;
    }

    /**
     * Move region
     * @param admin hbase admin
     * @param sregions 
     * @param index 
     * @param shostport 
     * @param dhostport 
     * @return True if region was moved, false otherwise
     * @throws Exception 
     */
    public boolean moveRegion(final HBaseAdmin admin, final HashMap<String, List<HRegionInfo>> sregions,
        final int index, final String shostport, final String dhostport) throws Exception {
        boolean ret = false;
        final HRegionInfo regionInfo = sregions.get(shostport).get(index);
        admin.move(regionInfo.getEncodedNameAsBytes(), serverNameMap.get(dhostport).getServerName().getBytes());

        if (isRegionLive(admin, regionInfo) && dhostport.equals(getServerHostingRegion(admin, regionInfo))) {
            sregions.get(shostport).remove(index);
            sregions.get(dhostport).add(regionInfo);
            ret = true;
        }
        
        return ret;
    }
    
    /**
     * Comparator to sort servers by region count
     */
    private class RSCountCompare implements Comparator<String> {
        @Override
        public int compare(final String hp1, final String hp2) {
            int ret = 0;
            int nr1 = getNumRegions(hp1);
            int nr2 = getNumRegions(hp2);
            
            if ((nr1 > 0) && (nr2 > 0)) {
                if (nr1 > nr2) {
                    ret = 1;
                } else if (nr2 > nr1) {
                    ret = -1;
                }
            } else if (nr1 > 0) {
                ret = 1;
            } else if (nr2 > 0) {
                ret = -1;
            }
            
            return ret;
        }
    }
}
