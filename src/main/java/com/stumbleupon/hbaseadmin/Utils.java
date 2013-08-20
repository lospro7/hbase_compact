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

import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for HBaseCompact
 */
final class Utils {

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    
    /**
     * Cannot construct
     */
    private Utils() {
        // Cannot be instantiated
    }
    
    /**
     * Convert a Date into a string
     * @param d The date object to convert
     * @param format The format
     * @return The formatted date string
     */
    public static String dateString(final Date d, final String format) {
        final SimpleDateFormat df = new SimpleDateFormat(format);
        return df.format(d);
    }

    /**
     * sleeps for sleepBetweenChecks milliseconds untill the current time-stamp is between start and stop.
     * @param start The start time to sleep
     * @param stop The end time to sleep
     * @param sleepBetweenChecks How long to sleep between checks
     * @throws InterruptedException 
     */
    public static void waitTillTime(final String start, final String stop, final int sleepBetweenChecks)
        throws InterruptedException {
        long iterations = 0;
        String now = dateString(new Date(), "HHmm");

        while ((now.compareTo(start) < 0) || (now.compareTo(stop) > 0)) {
            if (iterations == 0) {
                LOG.info("Waiting for off-peak hours...");
            }
            
            Thread.sleep(sleepBetweenChecks);
            now = dateString(new Date(), "HHmm");
            iterations++;
        }
        
        if (iterations != 0) {
            LOG.info("Resuming...");
        }
    }
}
