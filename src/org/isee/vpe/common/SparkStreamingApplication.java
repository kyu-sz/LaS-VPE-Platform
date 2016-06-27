package org.isee.vpe.common;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

public abstract class SparkStreamingApplication implements Serializable {

	private static final long serialVersionUID = 2780614096112566164L;

	protected transient JavaStreamingContext streamingContext = null;
	
	protected abstract JavaStreamingContext getStreamContext();
	
	public void initialize(String checkpointDir) {
		streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, new JavaStreamingContextFactory() {
			
			@Override
			public JavaStreamingContext create() {
				return getStreamContext();
			}
		});
	}
	
	public void start() {
		streamingContext.start();
	}
	
	public void stop() {
		streamingContext.stop();
	}
	
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
