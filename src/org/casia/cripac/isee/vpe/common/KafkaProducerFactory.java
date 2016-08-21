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

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class KafkaProducerFactory<K, V> extends ObjectFactory<KafkaProducer<K, V>> {

	private static final long serialVersionUID = 537687120172257949L;

	/**
	 * Configuration for constructing the object.
	 */
	private Properties config;

	/**
	 * Input a property for constructing the Kafka producer.
	 */
	public KafkaProducerFactory(Properties prop) {
		this.config = prop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.common.ObjectFactory#getObject(java.util.
	 * Properties)
	 */
	@Override
	public KafkaProducer<K, V> getObject() {
		return new KafkaProducer<>(config);
	}

}
