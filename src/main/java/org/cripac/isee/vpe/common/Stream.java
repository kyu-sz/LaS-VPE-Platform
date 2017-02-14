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

import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one INPUT_TYPE of output.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {

    private static final long serialVersionUID = 7965952554107861881L;

    /**
     * The Info class is designed to force the output data INPUT_TYPE to
     * be assigned to a stream, so that INPUT_TYPE matching checking can
     * be conducted.
     */
    public static class Info implements Serializable {
        private static final long serialVersionUID = -2859100367977900846L;
        /**
         * Name of the stream.
         */
        public final String NAME;

        /**
         * Type of output.
         */
        public final DataTypes OUTPUT_TYPE;

        /**
         * Construct a stream with NAME specified.
         *
         * @param name Name of the stream.
         */
        public Info(String name, DataTypes outputType) {
            this.NAME = name;
            this.OUTPUT_TYPE = outputType;
        }

        @Override
        public int hashCode() {
            return NAME.hashCode();
        }

        @Override
        public String toString() {
            return "Topic(name: \'" + NAME + "\', output type: \'" + OUTPUT_TYPE + "\')";
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Info) {
                assert OUTPUT_TYPE == ((Info) o).OUTPUT_TYPE;
                return NAME.equals(((Info) o).NAME);
            } else {
                return super.equals(o);
            }
        }
    }

    protected final Singleton<Logger> loggerSingleton;

    /**
     * Require inputting a logger singleton for this class and all its subclasses.
     *
     * @param loggerSingleton A singleton of logger.
     */
    public Stream(@Nonnull Singleton<Logger> loggerSingleton) {
        this.loggerSingleton = loggerSingleton;
    }

    /**
     * Append the stream to a Spark Streaming stream.
     *
     * @param globalStream A Spark Streaming stream.
     */
    public abstract void addToStream(JavaPairDStream<String, byte[]> globalStream);

    public abstract List<String> listeningTopics();
}
