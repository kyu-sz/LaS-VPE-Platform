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

import com.google.gson.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.URI;
import java.net.URISyntaxException;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imdecode;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;

/**
 * The HadoopHelper class provides utilities for Hadoop usage.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class HadoopHelper {

    static {
        // These two lines are used to solve the following problem:
        // RuntimeException: No native JavaCPP library
        // in memory. (Has Loader.load() been called?)
        Loader.load(org.bytedeco.javacpp.helper.opencv_core.class);
        Loader.load(opencv_imgproc.class);
    }

    public static Configuration getDefaultConf() {
        // Load Hadoop configuration from XML files.
        Configuration hadoopConf = new Configuration();
        String hadoopHome = System.getenv("HADOOP_HOME");
        hadoopConf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
        hadoopConf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
        hadoopConf.setBoolean("dfs.support.append", true);
        hadoopConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName(), "LaS-VPE Platform");
        hadoopConf.set("fs.file.impl", LocalFileSystem.class.getName(), "LaS-VPE Platform");
        return hadoopConf;
    }

    /**
     * Retrieve a tracklet from the HDFS or HAR.
     * Since a tracklet might be deleted from HDFS during reading,
     * it is highly recommended to retry this function on failure,
     * and the next time it will find the tracklet from HAR.
     *
     * @param storeDir the directory storing the tracklet (including only data of this tracklet).
     * @return the track retrieved.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     */
    @Nonnull
    public static Tracklet retrieveTracklet(@Nonnull String storeDir) throws IOException, URISyntaxException {
        return retrieveTracklet(storeDir, new HDFSFactory().produce());
    }

    /**
     * Retrieve a tracklet from the HDFS or HAR.
     * Since a tracklet might be deleted from HDFS during reading,
     * it is highly recommended to retry this function on failure,
     * and the next time it will find the tracklet from HAR.
     *
     * @param storeDir the directory storing the tracklet (including only data of this tracklet).
     * @return the track retrieved.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     */
    @Nonnull
    public static Tracklet retrieveTracklet(@Nonnull String storeDir,
                                            @Nonnull FileSystem hdfs) throws IOException, URISyntaxException {
        final InputStreamReader infoReader;
        final HarFileSystem harFS;
        final FileSystem fs;
        final String revisedStoreDir;

        boolean onHDFS = false;
        try {
            onHDFS = hdfs.exists(new Path(storeDir));
        } catch (IOException | IllegalArgumentException ignored) {
        }
        if (onHDFS) {
            infoReader = new InputStreamReader(hdfs.open(new Path(storeDir + "/info.txt")));
            fs = hdfs;
            revisedStoreDir = storeDir;
            harFS = null;
        } else {
            // Open the Hadoop Archive of the task the track is generated in.
            while (storeDir.endsWith("/")) {
                storeDir = storeDir.substring(0, storeDir.length() - 1);
            }
            if (storeDir.contains(".har")) {
                revisedStoreDir = storeDir;
            } else {
                final int splitter = storeDir.lastIndexOf("/");
                revisedStoreDir = storeDir.substring(0, splitter) + ".har" + storeDir.substring(splitter);
            }
            harFS = new HarFileSystem();
            harFS.initialize(new URI(revisedStoreDir), new Configuration());
            infoReader = new InputStreamReader(hdfs.open(new Path(revisedStoreDir + "/info.txt")));
            fs = harFS;
        }

        // Read verbal informations of the track.
        Gson gson = new Gson();
        Tracklet tracklet = gson.fromJson(infoReader, Tracklet.class);

        // Read frames concurrently..
        ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                .parallelStream()
                .forEach(idx -> {
                    Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];
                    FSDataInputStream imgInputStream;
                    final Path imgPath = new Path(revisedStoreDir + "/" + idx + ".jpg");
                    boolean isSample = false;
                    try {
                        isSample = fs.exists(imgPath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        if (isSample) {
                            imgInputStream = fs.open(imgPath);
                            byte[] rawBytes = IOUtils.toByteArray(imgInputStream);
                            imgInputStream.close();
                            opencv_core.Mat img = imdecode(new opencv_core.Mat(rawBytes), CV_8UC3);
                            bbox.patchData = new byte[img.rows() * img.cols() * img.channels()];
                            img.data().get(bbox.patchData);
                            img.release();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        if (harFS != null) {
            harFS.close();
        }
        return tracklet;
    }

    /**
     * Get the content of info.txt in Har.
     * 
     * @param  storeDir the directory storing the tracklet.
     * @throws IOException        on failure of retrieving the tracklet.
     * @throws URISyntaxException on syntax error detected in the storeDir.
     * @return the content in info.txt which is in json format (as a string).
     */
    public static String getTrackletInfo(@Nonnull String storeDir) throws IOException, URISyntaxException {
        final InputStreamReader infoReader;
        final HarFileSystem harFS;
        final String revisedStoreDir;

        FileSystem hdfs = new HDFSFactory().produce();

        boolean onHDFS = false;
        try {
            onHDFS = hdfs.exists(new Path(storeDir));
        } catch(IOException | IllegalArgumentException ignored) {
            
        }
        if (onHDFS) {
            infoReader = new InputStreamReader(hdfs.open(new Path(storeDir + "/info.txt")));
            harFS = null;
        } else {
            // Open the Hadoop Archive of the task the track is generated in.
            while (storeDir.endsWith("/")) {
                storeDir = storeDir.substring(0, storeDir.length() - 1);
            }
            if (storeDir.contains(".har")) {
                revisedStoreDir = storeDir;
            } else {
                final int splitter = storeDir.lastIndexOf("/");
                revisedStoreDir = storeDir.substring(0, splitter) + ".har" + storeDir.substring(splitter);
            }
            harFS = new HarFileSystem();
            // When we run the code on our platform, it maybe not necessary to call getDefaultConf().
            // And just use "new Configuration()" as the second input in harFS.initialize().
            //Configuration hdfsConf = getDefaultConf();
            //harFS.initialize(new URI(revisedStoreDir), hdfsConf);
            harFS.initialize(new URI(revisedStoreDir), new Configuration());
            infoReader = new InputStreamReader(harFS.open(new Path(revisedStoreDir + "/info.txt")));
        }

        BufferedReader bufferedReader = new BufferedReader(infoReader);
        String trackletInfo = bufferedReader.readLine();

        if (harFS != null) {
            harFS.close();
        }

        return trackletInfo;
    }

    /**
     * Store a tracklet to the HDFS.
     *
     * @param storeDir the directory storing the tracklet.
     * @param tracklet the tracklet to store.
     * @throws IOException on failure creating and writing files in HDFS.
     */
    public static void storeTracklet(@Nonnull String storeDir,
                                     @Nonnull Tracklet tracklet,
                                     @Nonnull FileSystem hdfs) throws Exception {
        // Write verbal informations with Json.
        final FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/info.txt"));

        // Customize the serialization of bounding box in order to ignore patch data.
        final GsonBuilder gsonBuilder = new GsonBuilder();
        final JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
            JsonObject result = new JsonObject();
            result.add("x", new JsonPrimitive(box.x));
            result.add("y", new JsonPrimitive(box.y));
            result.add("width", new JsonPrimitive(box.width));
            result.add("height", new JsonPrimitive(box.height));
            return result;
        };
        gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);

        // Write serialized basic information of the tracklet to HDFS.
        outputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
        outputStream.close();

        // Write frames concurrently.
        ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                .parallelStream()
                // Find bounding boxes that contain patch data.
                .filter(idx -> tracklet.locationSequence[idx].patchData != null)
                .forEach(idx -> {
                    final Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];

                    // Use JavaCV to encode the image patch
                    // into JPEG, stored in the memory.
                    final BytePointer inputPointer = new BytePointer(bbox.patchData);
                    final opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
                    final BytePointer outputPointer = new BytePointer();
                    imencode(".jpg", image, outputPointer);
                    final byte[] bytes = new byte[(int) outputPointer.limit()];
                    outputPointer.get(bytes);

                    // Output the image patch to HDFS.
                    final FSDataOutputStream imgOutputStream;
                    try {
                        imgOutputStream = hdfs.create(new Path(storeDir + "/" + idx + ".jpg"));
                        imgOutputStream.write(bytes);
                        imgOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    // Free resources.
                    image.release();
                    inputPointer.deallocate();
                    outputPointer.deallocate();
                });
    }
}
