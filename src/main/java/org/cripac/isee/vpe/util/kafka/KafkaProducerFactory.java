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

package org.cripac.isee.vpe.util.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.cripac.isee.vpe.util.Factory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Object factory for producing Kafka producers with a fixed configuration.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class KafkaProducerFactory<K, V> implements Factory<KafkaProducer<K, V>> {

    private static final long serialVersionUID = 537687120172257949L;

    /**
     * Configuration for constructing the object.
     */
    private Properties config;

    /**
     * Input a property for constructing the Kafka producer.
     */
    public KafkaProducerFactory(@Nullable Properties prop) {
        if (prop == null) {
            this.config = prop;
        } else {
            this.config = new Properties();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.casia.cripac.isee.vpe.common.ObjectFactory#getObject(java.util.
     * Properties)
     */
    @Override
    public KafkaProducer<K, V> produce() {
        return new KafkaProducer<>(config);
    }

}
