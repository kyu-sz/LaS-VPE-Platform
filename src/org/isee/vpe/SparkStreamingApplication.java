package org.isee.vpe;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

public abstract class SparkStreamingApplication implements Serializable {

	private static final long serialVersionUID = 2780614096112566164L;

	protected transient JavaStreamingContext streamingContext = null;
	protected HashSet<String> topicsSet = new HashSet<>();
	
	protected abstract JavaStreamingContext getStreamContext(String brokers);
	protected Set<String> getRequiredTopics() {
		return topicsSet;
	}
	
	public SparkStreamingApplication(String brokers) {
		streamingContext = getStreamContext(brokers);
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
		streamingContext.close();
		super.finalize();
	}
}
