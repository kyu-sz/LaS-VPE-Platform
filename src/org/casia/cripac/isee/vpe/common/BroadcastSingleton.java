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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * The BroadcastSingleton class is an ultimate solution for using unserializable objects in Spark Streaming!
 * @author Ken Yu, CRIPAC, 2016
 *
 * @param <T>	The type of the object.
 */
public class BroadcastSingleton<T> implements Serializable {
	
	private static final long serialVersionUID = -7565726994857167434L;

	/**
	 * Lazy-evaluated object.
	 */
	@SuppressWarnings("rawtypes")
	private static volatile Map<Class, Broadcast<Object>> broadcastPool = null;
	private ObjectFactory<T> objectFactory = null;
	final private Class<T> typeParameterClass;
	
	/**
	 * Constructor of BroadcastSingleton.
	 * @param objFactory	Factory that provides method to generate a new object. 
	 * @param clazz			The class of the object.
	 */
	@SuppressWarnings("unchecked")
	public BroadcastSingleton(ObjectFactory<T> objFactory, @SuppressWarnings("rawtypes") Class clazz) {
		this.objectFactory = objFactory;
		this.typeParameterClass = clazz;
	}

	/**
	 * Get a supplier of the object.
	 * The supplier is broadcast over the cluster, and provides a singleton of the object.
	 * The broadcast is lazy-evaluated, so it cannot be checkpointed by Spark Streaming.
	 * Every time the application starts or recovers, the first call of this method would broadcast a supplier.
	 * @param sparkContext	The Spark context for broadcasting.
	 * @return				A supplier for the object.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ObjectSupplier<T> getSupplier(JavaSparkContext sparkContext) {
		if (broadcastPool == null) {
			synchronized (BroadcastSingleton.class) {
				if (broadcastPool == null) {
					broadcastPool = new HashMap<Class, Broadcast<Object>>();
				}
			}
		}
		if (!broadcastPool.containsKey(typeParameterClass)) {
			synchronized (BroadcastSingleton.class) {
				if (!broadcastPool.containsKey(typeParameterClass)) {
					broadcastPool.put(typeParameterClass, sparkContext.broadcast(new ObjectSupplier<>(objectFactory)));
				}
			}
		}
		return (ObjectSupplier<T>) broadcastPool.get(typeParameterClass).value();
	}
};
