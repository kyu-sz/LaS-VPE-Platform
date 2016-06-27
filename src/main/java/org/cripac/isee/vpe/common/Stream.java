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

import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one TYPE of output.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {
    /**
     * The Info class is designed to force the output data TYPE to
     * be assigned to a stream, so that TYPE matching checking can
     * be conducted.
     */
    public static class Info implements Serializable {
        /**
         * Name of the stream.
         */
        public final String NAME;

        /**
         * Type of output.
         */
        public final DataType OUTPUT_TYPE;

        /**
         * Construct a stream with NAME specified.
         *
         * @param name Name of the stream.
         */
        public Info(String name, DataType outputType) {
            this.NAME = name;
            this.OUTPUT_TYPE = outputType;
        }

        @Override
        public int hashCode() {
            return NAME.hashCode();
        }

        @Override
        public String toString() {
            return "[" + OUTPUT_TYPE + "]" + NAME;
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

    /**
     * Add the stream to a Spark Streaming context.
     *
     * @param jsc A Spark Streaming context.
     */
    public abstract void addToContext(JavaStreamingContext jsc);
}
