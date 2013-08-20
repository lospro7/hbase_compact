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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles offline HBase compactions - runs compactions between pre-set times.
 */
public class HBaseCompact {

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(HBaseCompact.class);
    /** default */
    private static final int DEFAULT_PAUSE_INTERVAL = 30000;
    /** default */
    private static final int DEFAULT_WAIT_INTERVAL = 60000;
    /** default */
    private static final int DEFAULT_FILES_KEEP = 3;
    /** default 24hrs */
    private static final long DEFAULT_REGION_COMPACT_WAIT_TIME = 86400000;
    
    /** param */
    private int throttleFactor;
    /** param */
    private int numCycles;
    /** param */
    private int sleepBetweenCompacts;
    /** param */
    private int sleepBetweenChecks;
    /** param */
    private double skipFactor = 0.0;
    /** param */
    private HBaseAdmin admin;
    /** param */
    private Date startDate = null;
    /** param */
    private Date endDate = null;
    /** param */
    private int numStoreFiles = 3;
    /** param */
    private long maxStoreFileAge = 0;
    /** param */
    private List<String> tableNames = new ArrayList<String>();
    /** param */
    private boolean excludeTables = false;
    
    /** The cluster status */
    private ClusterUtils clusterUtils = null;

    /**
     * Main entry point
     * @param args command line arguments
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        String hbaseSite = null;
        String jmxRemotePasswordFile = null;
        String jmxPort = null;
        Date startDate = null;
        Date endDate = null;
        int throttleFactor = 1;
        int numCycles = 1;
        int pauseInterval = DEFAULT_PAUSE_INTERVAL;
        int waitInterval = DEFAULT_WAIT_INTERVAL;
        int filesKeep = DEFAULT_FILES_KEEP;
        long regionCompactWaitTime = DEFAULT_REGION_COMPACT_WAIT_TIME;
        long maxStoreFileAge = 0;
        boolean excludeTables = false;
        String tableNamesString = "";
        List<String> tableNames = new ArrayList<String>();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        
        // Parse command line options
        try {
            cmd = parser.parse(getOptions(), args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printOptions();
            System.exit(-1);
        }
        
        for (Option option : cmd.getOptions()) {
            switch (option.getId()) {
            case 'c':
                hbaseSite = option.getValue();
                break;
            case 'j':
                jmxRemotePasswordFile = option.getValue();
                break;
            case 't':
                throttleFactor = Integer.parseInt(option.getValue());
                break;
            case 'n':
                numCycles = Integer.parseInt(option.getValue());
                break;
            case 'p':
                pauseInterval = Integer.parseInt(option.getValue());
                break;
            case 'w':
                waitInterval = Integer.parseInt(option.getValue());
                break;
            case 's':
                startDate = sdf.parse(option.getValue());
                break;
            case 'e':
                endDate = sdf.parse(option.getValue());
                break;
            case 'b':
                tableNamesString = option.getValue();
                tableNames = Arrays.asList(option.getValue().split(","));
                break;
            case 'f':
                filesKeep = Integer.parseInt(option.getValue());
                break;
            case 'r':
                jmxPort = option.getValue();
                break;
            case 'x':
                excludeTables = true;
                break;
            case 'm':
                regionCompactWaitTime = Long.parseLong(option.getValue());
                break;
            case 'a':
                maxStoreFileAge = Long.parseLong(option.getValue());
                break;
            default:
                throw new IllegalArgumentException("unexpected option " + option);
            }
        }

        LOG.info("Starting compactor");
        LOG.info("--------------------------------------------------");
        LOG.info("HBase site              : {}", hbaseSite);
        LOG.info("RegionServer Jmx port   : {}", jmxPort);
        LOG.info("Jmx password file       : {}", jmxRemotePasswordFile);
        LOG.info("Compact interval        : {}", pauseInterval);
        LOG.info("Check interval          : {}", waitInterval);
        LOG.info("Throttle factor         : {}", throttleFactor);
        LOG.info("Number of cycles        : {}", numCycles);
        LOG.info("Off-peak start time     : {}", Utils.dateString(startDate, "HH:mm"));
        LOG.info("Off-peak end time       : {}", Utils.dateString(endDate, "HH:mm"));
        LOG.info("Minimum store files     : {}", filesKeep);
        LOG.info("Table names             : {}", tableNamesString);
        LOG.info("Exclude tables          : {}", excludeTables);
        LOG.info("Region compact wait time: {}", regionCompactWaitTime);
        LOG.info("Max store file age      : {}", maxStoreFileAge);
        LOG.info("--------------------------------------------------");
        
        // Get command line options
        final Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(hbaseSite));
        
        HBaseCompact compact = new HBaseCompact();
        ClusterUtils clusterUtils = new ClusterUtils(compact, regionCompactWaitTime);

        compact.setClusterUtils(clusterUtils);
        compact.setAdmin(new HBaseAdmin(conf));
        compact.setSleepBetweenCompacts(pauseInterval);
        compact.setSleepBetweenChecks(waitInterval);
        compact.setThrottleFactor(throttleFactor);
        compact.setNumCycles(numCycles);
        compact.setStartDate(startDate);
        compact.setEndDate(endDate);
        compact.setNumStoreFiles(filesKeep);
        compact.setTableNames(tableNames);
        compact.setExcludeTables(excludeTables);
        compact.setMaxStoreFileAge(maxStoreFileAge);
        
        clusterUtils.setJmxPort(jmxPort);
        clusterUtils.setJmxPasswordFile(jmxRemotePasswordFile);
        
        compact.runCompactions();
    }
    
    /**
     * Main loop
     * @throws Exception 
     */
    public void runCompactions() throws Exception {
        int iteration = 0;
        long prevNumRegions = 0;
        final String startHHmm = Utils.dateString(startDate, "HHmm");
        final String stopHHmm = Utils.dateString(endDate, "HHmm");

        LOG.info("Looking for regions to compact...");
        if (numCycles == 0) {
            iteration = -1;
        }
        
        while (iteration < numCycles) {
            Utils.waitTillTime(startHHmm, stopHHmm, sleepBetweenChecks);
            clusterUtils.updateForNextRun();

            if (prevNumRegions > 0) {
                LOG.info("Looking for regions to compact...");
            }
            
            long numRegions = compactAllServers();
            if (numCycles > 0) {
                ++iteration;
            }
            
            try {
                Thread.sleep(DEFAULT_PAUSE_INTERVAL);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while sleeping for next compaction run", e);
            }
            
            prevNumRegions = numRegions;
        }
    }
    
    /**
     * cycles through all the servers and compacts regions. We process one region on each server, delete it from the
     * region list, compact it, move on to the next server and so on. Once we are done with a server sweep across all of
     * them, we start over and repeat utill we are done with the regions.
     * @return The total number of regions that we tried compacting
     */
    private long compactAllServers() {
        final String startHHmm = Utils.dateString(startDate, "HHmm");
        final String stopHHmm = Utils.dateString(endDate, "HHmm");
        long regionsCompacted = 0;
        long regionsFailed = 0;

        try {
            Set<String> regionNames = clusterUtils.getRegionNames(admin);

            if (!regionNames.isEmpty()) {
                LOG.info("Starting compaction run-through");
            }
            
            while (!regionNames.isEmpty()) {
                for (final String regionName : regionNames) {
                    String hostport = clusterUtils.getRegionHostPort(regionName);
                    
                    try {
                        if (skipFactor <= Math.random()) {
                            Utils.waitTillTime(startHHmm, stopHHmm, sleepBetweenChecks);
                            HRegionInfo region = clusterUtils.getNextRegion(regionName, throttleFactor);
                            
                            if (region != null) {
                                LOG.info("Compacting {} [{}]", hostport, regionName);
                                HRegionInterface regionServer = clusterUtils.getServerInterfaceMap().get(hostport);
                                regionServer.compactRegion(region, true);
                                regionsCompacted++;
                            }
                        } else {
                            LOG.info("Skipping compactions on {} because it's not in the cards this time.",
                                hostport);
                        }
                    } catch (IOException e) {
                        regionsFailed++;
                        LOG.error("Failed to compact '{}'", regionName, e);
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for non-peak time", e);
                    } catch (Exception e) {
                        regionsFailed++;
                        LOG.error("Unexpected error. Failed to compact '{}'", regionName, e);
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Regions compacted: {} Failed: {}. Sleeping " + sleepBetweenCompacts + "ms",
                        regionsCompacted, regionsFailed);
                }
                
                try {
                    Thread.sleep(sleepBetweenCompacts);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting to run next set of regions", e);
                }
                
                regionNames = clusterUtils.getRegionNames(admin);
            }
            
            if (regionsCompacted > 0 || regionsFailed > 0) {
                LOG.info("Done with compaction run-though. Regions compacted: {} Failed: {}",
                    regionsCompacted, regionsFailed);
            }
        } catch (IOException e) {
            LOG.error("Could not get a list of region names from cluster", e);
        }
        
        return regionsCompacted + regionsFailed;
    }

    /**
     * Get the throttle factor
     * @return the throttleFactor
     */
    public int getThrottleFactor() {
        return throttleFactor;
    }

    /**
     * Set the throttle factor
     * @param throttleFactor the throttleFactor to set
     */
    public void setThrottleFactor(int throttleFactor) {
        this.throttleFactor = throttleFactor;
    }

    /**
     * Get the number of cycles
     * @return the numCycles
     */
    public int getNumCycles() {
        return numCycles;
    }

    /**
     * Set the number of cycles
     * @param numCycles the numCycles to set
     */
    public void setNumCycles(int numCycles) {
        this.numCycles = numCycles;
    }

    /**
     * Get the time to sleep between compactions
     * @return the sleepBetweenCompacts
     */
    public int getSleepBetweenCompacts() {
        return sleepBetweenCompacts;
    }

    /**
     * Set the time to sleep between compactions
     * @param sleepBetweenCompacts the sleepBetweenCompacts to set
     */
    public void setSleepBetweenCompacts(int sleepBetweenCompacts) {
        this.sleepBetweenCompacts = sleepBetweenCompacts;
    }

    /**
     * Get the time to sleep between checks
     * @return the sleepBetweenChecks
     */
    public int getSleepBetweenChecks() {
        return sleepBetweenChecks;
    }

    /**
     * Set the time to sleep between checks
     * @param sleepBetweenChecks the sleepBetweenChecks to set
     */
    public void setSleepBetweenChecks(int sleepBetweenChecks) {
        this.sleepBetweenChecks = sleepBetweenChecks;
    }

    /**
     * Get the skip factor
     * @return the skipFactor
     */
    public double getSkipFactor() {
        return skipFactor;
    }

    /**
     * Set the skip factor
     * @param skipFactor the skipFactor to set
     */
    public void setSkipFactor(double skipFactor) {
        this.skipFactor = skipFactor;
    }

    /**
     * Get the HBaseAdmin
     * @return the admin
     */
    public HBaseAdmin getAdmin() {
        return admin;
    }

    /**
     * Set the HBaseAdmin
     * @param admin the admin to set
     */
    public void setAdmin(HBaseAdmin admin) {
        this.admin = admin;
    }

    /**
     * Get the off peak start time
     * @return the startDate
     */
    public Date getStartDate() {
        return startDate;
    }

    /**
     * Set the off peak start time
     * @param startDate the startDate to set
     */
    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    /**
     * Get the off peak end time
     * @return the endDate
     */
    public Date getEndDate() {
        return endDate;
    }

    /**
     * Set the off peak end time
     * @param endDate the endDate to set
     */
    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
    
    /**
     * Get the number of store files needed to perform a compaction
     * @return the numStoreFiles
     */
    public int getNumStoreFiles() {
        return numStoreFiles;
    }

    /**
     * Set the number of store files needed to perform a compaction
     * @param numStoreFiles the numStoreFiles to set
     */
    public void setNumStoreFiles(int numStoreFiles) {
        this.numStoreFiles = numStoreFiles;
    }

    /**
     * Get the table names to compact
     * @return the tableNames
     */
    public List<String> getTableNames() {
        return tableNames;
    }

    /**
     * Set the table names to compact
     * @param tableNames the tableNames to set
     */
    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    /**
     * Returns true if tableNames list is treated as a list of tables to exclude from compaction
     * @return the excludeTables
     */
    public boolean getExcludeTables() {
        return excludeTables;
    }

    /**
     * Set to true if tableNames list should be treated as a list of tables to exclude from compaction
     * @param excludeTables the excludeTables to set
     */
    public void setExcludeTables(boolean excludeTables) {
        this.excludeTables = excludeTables;
    }

    /**
     * Get the cluster utils
     * @return the cluster
     */
    public ClusterUtils getClusterUtils() {
        return clusterUtils;
    }

    /**
     * Set the cluster utils
     * @param cluster the cluster to set
     */
    public void setClusterUtils(ClusterUtils cluster) {
        this.clusterUtils = cluster;
    }
    
    /**
     * Get the maximum store file age
     * @return the maxStoreFileAge
     */
    public long getMaxStoreFileAge() {
        return maxStoreFileAge;
    }

    /**
     * Set the maximum store file age 
     * @param maxStoreFileAge the maxStoreFileAge to set
     */
    public void setMaxStoreFileAge(long maxStoreFileAge) {
        this.maxStoreFileAge = maxStoreFileAge;
    }

    /**
     * Returns the command-line options supported.
     * 
     * @return the command-line options
     */
    private static Options getOptions() {
        Options options = new Options();

        Option hbaseSite = new Option("c", "hbaseSite", true,
            "Path to hbase-site.xml");
        Option jmxRemotePass = new Option("j", "jmsRemotePassword", true,
            "Path to jmxremote.password");
        Option throttleFactor = new Option("t", "throttleFactor", true,
            "Throttle factor to limit the compaction queue.  The default (1) limits it to num threads / 1");
        Option numCycles = new Option("n", "numCycles", true,
            "Number of iterations to run.  The default is 1.  Set to 0 to run forever");
        Option pauseInterval = new Option("p", "pauseInterval", true,
            "Time (in milliseconds) to pause between compactions");
        Option waitInterval = new Option("w", "waitInterval", true,
            "Time (in milliseconds) to wait between time (are we there yet?) checks");
        Option startTime = new Option("s", "startTime", true,
            "Time to start compactions. Format is hh:mm");
        Option endTime = new Option("e", "endTime", true,
            "Time to stop compactions. Format is hh:mm");
        Option tableNames = new Option("b", "tableNames", true,
            "Comma-delimited list. Specific table names to check against (default is all)");
        Option filesKeep = new Option("f", "filesKeep", true,
            "Number of storefiles to look for before compacting (default is 5)");
        Option jmxPort = new Option("r", "jmxPort", true,
            "The remote jmx port number");
        Option excludeTables = new Option("x", "excludeTables", false,
            "Treat --tableNames option as a list of tables to exclude from compaction");
        Option regionCompactWaitTime = new Option("m", "regionCompactWaitTime", true,
            "The time in ms to wait before the compaction of the same region. Set to -1 to not wait");
        Option maxStoreFileAge = new Option("a", "maxStoreFileAge", true,
            "Force compaction of a region when its oldest file is older than this value. Default: 0 (disabled)");
        
        hbaseSite.setRequired(true);
        jmxRemotePass.setRequired(false);
        throttleFactor.setRequired(false);
        numCycles.setRequired(false);
        pauseInterval.setRequired(false);
        waitInterval.setRequired(false);
        startTime.setRequired(true);
        endTime.setRequired(true);
        tableNames.setRequired(false);
        filesKeep.setRequired(false);
        jmxPort.setRequired(true);
        excludeTables.setRequired(false);
        regionCompactWaitTime.setRequired(false);
        maxStoreFileAge.setRequired(false);
        
        options.addOption(hbaseSite);
        options.addOption(jmxRemotePass);
        options.addOption(throttleFactor);
        options.addOption(numCycles);
        options.addOption(pauseInterval);
        options.addOption(waitInterval);
        options.addOption(startTime);
        options.addOption(endTime);
        options.addOption(tableNames);
        options.addOption(filesKeep);
        options.addOption(jmxPort);
        options.addOption(excludeTables);
        options.addOption(regionCompactWaitTime);
        options.addOption(maxStoreFileAge);
        
        return options;
    }
    
    /**
     * Print the available options to the display.
     */
    private static void printOptions() {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Run compactions across regions servers during off peak hours";
        formatter.printHelp("HBaseCompact", header, getOptions(), "", true);
    }
}
