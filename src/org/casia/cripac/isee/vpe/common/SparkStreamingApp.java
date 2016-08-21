/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.casia.cripac.isee.vpe.common;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * The SparkStreamingApp class wraps a whole Spark Streaming application,
 * including driver code and executor code. After initialized, it can be used
 * just like a JavaStreamingContext class. Note that you must call the
 * initialize() method after construction and before using it.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class SparkStreamingApp implements Serializable {

	private static final long serialVersionUID = 2780614096112566164L;

	/**
	 * Common Spark Streaming context variable.
	 */
	private transient JavaStreamingContext streamingContext = null;

	/**
	 * Implemented by subclasses, this method produces an application-specified
	 * Spark Streaming context.
	 * 
	 * @return An application-specified Spark Streaming context.
	 */
	protected abstract JavaStreamingContext getStreamContext();

	abstract public String getAppName();

	/**
	 * Initialize the application.
	 * 
	 * @param checkpointRootDir
	 *            The directory storing the check points of Spark Streaming
	 *            context.
	 */
	public void initialize(SystemPropertyCenter propertyCenter) {

		SynthesizedLogger logger = new SynthesizedLogger(propertyCenter.messageListenerAddress,
				propertyCenter.messageListenerPort);

		String checkpointDir = propertyCenter.checkpointRootDir + "/" + getAppName();
		logger.info("Using " + checkpointDir + " as checkpoint directory.");
		streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, new Function0<JavaStreamingContext>() {

			private static final long serialVersionUID = 1136960093227848775L;

			@Override
			public JavaStreamingContext call() throws Exception {
				JavaStreamingContext context = getStreamContext();
				try {
					if (propertyCenter.sparkMaster.contains("local")) {
						File dir = new File(checkpointDir);
						dir.delete();
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
			}
		}, new Configuration(), true);
	}

	/**
	 * Start the application.
	 */
	public void start() {
		streamingContext.start();
	}

	/**
	 * Stop the application.
	 */
	public void stop() {
		streamingContext.stop();
	}

	/**
	 * Await termination of the application.
	 */
	public void awaitTermination() {
		streamingContext.awaitTermination();
	}

	@Override
	protected void finalize() throws Throwable {
		if (streamingContext != null) {
			streamingContext.close();
		}
		super.finalize();
	}
}
