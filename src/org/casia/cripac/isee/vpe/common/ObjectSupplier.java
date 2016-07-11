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

import java.io.Closeable;
import java.io.Serializable;
import java.util.function.Supplier;

/**
 * The ObjectSupplier class is a smart wrapper for any objects.
 * It initializes an object just before using it, like lazy evaluation in Scala.
 * The wrapped object is usually not serializable, so it cannot be broadcast to Spark Streaming executors.
 * But this wrapper can be serialized, so it can bring the wrapped object to executors.
 * To use it with Spark Streaming, first use SparkContext to create a broadcast variable of ObjectSink:
 * <pre>
 * {@code
 * Broadcast<ObjectSupplier<T>> broadcastSupplier = sparkContext.broadcast(new ObjectSupplier<T>(config));
 * }
 * </pre>
 * Then in an executor, get a ObjectSupplier from the broadcast and get the wrapper object from it.
 * <pre>
 * {@code
 * T obj = broadcastSupplier.value().getObject();
 * }
 * </pre>
 * Then use the object as usual.
 * @author Ken Yu, CRIPAC, 2016
 *
 * @param T The type of the object to supply.
 */
public class ObjectSupplier<T> implements Serializable, Supplier<T> {
	private static final long serialVersionUID = -7565726994857167434L;

	/**
	 * Lazy-evaluated object.
	 */
	private transient T obj = null;
	private ObjectFactory<T> objectFactory = null;
	
	/**
	 * Constructor inputting a configuration for initializing KafkaProducer.
	 * The producer is not initialized immediately here, but lazy-evaluated somewhere else.
	 * @param config The configuration for KafkaProducer.
	 */
	public ObjectSupplier(ObjectFactory<T> objFactory) {
		this.objectFactory = objFactory;
	}
	
	@Override
	protected void finalize() throws Throwable {
		if (obj != null && obj instanceof Closeable)
			((Closeable) obj).close();
		
		super.finalize();
	}

	/* (non-Javadoc)
	 * @see java.util.function.Supplier#get()
	 */
	@Override
	public T get() {
		if (obj == null) {
			obj = objectFactory.getObject();
		}
		return obj;
	}
};
