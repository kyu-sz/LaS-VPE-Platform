/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cripac.isee.vpe.util.hdfs;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Level;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imdecode;

/**
 * The HadoopHelper class provides utilities for Hadoop usage.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class HadoopHelper {

    public static Configuration getDefaultConf()
            throws ParserConfigurationException, SAXException, IOException {
        // Load Hadoop configuration from XML files.
        Configuration hadoopConf = new Configuration();
        Map<String, String> propMap;
        String hadoopHome = System.getenv("HADOOP_HOME");
        propMap = HadoopXMLParser.getPropsFromXML(new File(hadoopHome + "/etc/hadoop/core-site.xml"));
        for (Entry<String, String> prop : propMap.entrySet()) {
            System.out.printf("Setting hadoop configuration: %s=%s\n", prop.getKey(), prop.getValue());
            hadoopConf.set(prop.getKey(), prop.getValue());
        }
        propMap = HadoopXMLParser.getPropsFromXML(new File(hadoopHome + "/etc/hadoop/yarn-site.xml"));
        for (Entry<String, String> prop : propMap.entrySet()) {
            System.out.printf("Setting hadoop configuration: %s=%s\n", prop.getKey(), prop.getValue());
            hadoopConf.set(prop.getKey(), prop.getValue());
        }
        // According to Da Li, this is necessary for some hadoop environments,
        // otherwise the error "No Filesystem for scheme: hdfs" will be raised.
        hadoopConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        return hadoopConf;
    }

    /**
     * Retrieve a track from the HDFS.
     *
     * @param storeDir The directory storing the tracklets.
     * @param id       The identifier of the track.
     * @return The track retrieved.
     */
    public static Tracklet retrieveTracklet(@Nonnull String storeDir,
                                            @Nonnull Tracklet.Identifier id,
                                            @Nullable Logger logger) {
        if (logger == null) {
            logger = new ConsoleLogger(Level.INFO);
        }
        try {
            // Open the Hadoop Archive of the task the track is generated in.
            HarFileSystem harFileSystem = new HarFileSystem();
            harFileSystem.initialize(new URI(storeDir), new Configuration());

            // Read verbal informations of the track.
            Gson gson = new Gson();
            Tracklet tracklet = gson.fromJson(
                    new InputStreamReader(harFileSystem.open(
                            new Path(storeDir + "/" + id.serialNumber + "/info.txt"))),
                    Tracklet.class);

            // Read frames concurrently..
            ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                    .parallelStream()
                    .forEach(idx -> {
                        Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];
                        FSDataInputStream imgInputStream = null;
                        try {
                            imgInputStream = harFileSystem
                                    .open(new Path(storeDir + "/" + id.toString() + "/" + idx + ".jpg"));
                            byte[] rawBytes = IOUtils.toByteArray(imgInputStream);
                            imgInputStream.close();
                            opencv_core.Mat img = imdecode(new opencv_core.Mat(rawBytes), CV_8UC3);
                            bbox.patchData = new byte[img.rows() * img.cols() * img.channels()];
                            img.data().get(bbox.patchData);
                            img.release();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
            harFileSystem.close();
            return tracklet;
        } catch (Exception e) {
            try {
                logger.error("Error when retrieving tracklets"
                        + " from \"" + storeDir + "/" + id.serialNumber + "/info.txt\".", e);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            return new FakePedestrianTracker().track(null)[0];
        }
    }
}
