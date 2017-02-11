/***********************************************************************
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
 ************************************************************************/

package org.cripac.isee.vpe.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * The SparkStreamingApp class wraps a whole Spark Streaming application,
 * including driver code and executor code. After initialized, it can be used
 * just like a JavaStreamingContext class. Note that you must call the
 * initialize() method after construction and before using it.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class SparkStreamingApp implements Serializable {

    private static final long serialVersionUID = 3098753124157119358L;
    private int batchDuration = 1000;
    private String checkpointRootDir;
    private String sparkMaster;

    public SparkStreamingApp(SystemPropertyCenter propCenter) {
        batchDuration = propCenter.batchDuration;
        checkpointRootDir = propCenter.checkpointRootDir;
        sparkMaster = propCenter.sparkMaster;
    }

    /**
     * Common Spark Streaming context variable.
     */
    private transient JavaStreamingContext jssc = null;

    /**
     * This method produces an empty Spark Streaming context.
     * Should be further implemented by subclasses to produce an application-specified context.
     *
     * @return An empty Spark Streaming context.
     */
    protected JavaStreamingContext getStreamContext() {
        // Create contexts.
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf(true));
        sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
        return new JavaStreamingContext(sparkContext, Durations.milliseconds(batchDuration));
    }

    abstract public String getAppName();

    /**
     * Initialize the application.
     */
    public void initialize() {
        String checkpointDir = checkpointRootDir + "/" + getAppName();
        jssc = JavaStreamingContext.getOrCreate(checkpointDir, () -> {
            JavaStreamingContext context = getStreamContext();
            try {
                if (sparkMaster.contains("local")) {
                    File dir = new File(checkpointDir);
                    //noinspection ResultOfMethodCallIgnored
                    dir.delete();
                    //noinspection ResultOfMethodCallIgnored
                    dir.mkdirs();
                } else {
                    FileSystem fs = FileSystem.get(new Configuration());
                    Path dir = new Path(checkpointDir);
                    fs.delete(dir, true);
                    fs.mkdirs(dir);
                }
                context.checkpoint(checkpointDir);
            } catch (IllegalArgumentException | IOException e) {
                e.printStackTrace();
            }
            return context;
        }, new Configuration(), true);
    }

    /**
     * Start the application.
     */
    public void start() {
        jssc.start();
    }

    /**
     * Stop the application.
     */
    public void stop() {
        jssc.stop();
    }

    /**
     * Await termination of the application.
     */
    public void awaitTermination() {
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (jssc != null) {
            jssc.close();
        }
        super.finalize();
    }
}
