package org.cripac.isee.vpe.util.kafka;/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-18.
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Properties;

public class ByteArrayProducer extends KafkaProducer<String, byte[]> {
    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     *
     * @param configs The producer configs
     */
    public ByteArrayProducer(Map<String, Object> configs) {
        super(configs);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     *
     * @param configs         The producer configs
     * @param keySerializer   The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                        called in the producer when the serializer is passed in directly.
     * @param valueSerializer The serializer for value that implements {@link Serializer}. The configure() method won't
     */
    public ByteArrayProducer(Map<String, Object> configs, Serializer<String> keySerializer, Serializer<byte[]> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     *
     * @param properties The producer configs
     */
    public ByteArrayProducer(Properties properties) {
        super(properties);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     *
     * @param properties      The producer configs
     * @param keySerializer   The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                        called in the producer when the serializer is passed in directly.
     * @param valueSerializer The serializer for value that implements {@link Serializer}. The configure() method won't
     */
    public ByteArrayProducer(Properties properties, Serializer<String> keySerializer, Serializer<byte[]> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }
}
