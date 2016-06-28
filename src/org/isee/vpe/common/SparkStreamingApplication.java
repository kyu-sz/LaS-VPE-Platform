package org.isee.vpe.common;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

/**
 * The SparkStreamingApplication class wraps a whole Spark Streaming application,
 * including driver code and executor code.
 * After initialized, it can be used just like a JavaStreamingContext class.
 * Note that you must call the initialize() method after construction and before using it.  
 * @author ken
 *
 */
public abstract class SparkStreamingApplication implements Serializable {

	private static final long serialVersionUID = 2780614096112566164L;

	/**
	 * Common Spark Streaming context variable.
	 */
	private transient JavaStreamingContext streamingContext = null;
	
	/**
	 * Implemented by subclasses, this method produces an application-specified Spark Streaming context.
	 * @return An application-specified Spark Streaming context.
	 */
	protected abstract JavaStreamingContext getStreamContext();
	
	/**
	 * Initialize the application.
	 * @param checkpointDir The directory storing the check points of Spark Streaming context.
	 */
	public void initialize(String checkpointDir) {
		streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, new JavaStreamingContextFactory() {
			
			@Override
			public JavaStreamingContext create() {
				return getStreamContext();
			}
		});
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
