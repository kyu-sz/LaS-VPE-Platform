package org.cripac.isee.vpe.common;/***********************************************************************
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

import org.cripac.isee.vpe.ctrl.TopicManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * The Topic class is a wrapper for Kafka topics with additional information.
 * <p>
 * Created by ken.yu on 16-10-5.
 */
public final class Topic implements Serializable {
    /**
     * Name of the topic to appear in Kafka.
     */
    public final String NAME;
    /**
     * Type of the topic used within the system.
     */
    public final DataType INPUT_TYPE;
    /**
     * Name of stream this topic belongs to.
     */
    public final Stream.Info STREAM_INFO;

    /**
     * Create a topic.
     *
     * @param name    Name of the topic to appear in Kafka.
     * @param type    Type of the topic used within the system.
     * @param streamInfo Information of stream this topic belongs to.
     *                   If set as null, the topic is dangling from any streams,
     *                   which is only used within external tests.
     */
    public Topic(@Nonnull String name,
                 @Nonnull DataType type,
                 @Nullable Stream.Info streamInfo) {
        this.NAME = name;
        this.INPUT_TYPE = type;
        this.STREAM_INFO = streamInfo;
        TopicManager.registerTopic(this);
    }

    /**
     * Transform the topic into a string in format as "[INPUT_TYPE]NAME".
     *
     * @return String representing the topic.
     */
    @Override
    public String toString() {
        return "[" + INPUT_TYPE + "]" + NAME;
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }
}
