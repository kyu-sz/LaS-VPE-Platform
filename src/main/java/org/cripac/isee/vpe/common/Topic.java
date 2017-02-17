package org.cripac.isee.vpe.common;/*
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
 */

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * The Topic class is a wrapper for Kafka topics with additional information.
 * <p>
 * Created by ken.yu on 16-10-5.
 */
public final class Topic implements Serializable {
    private static final long serialVersionUID = -7567029992452814611L;
    /**
     * Name of the topic to appear in Kafka.
     */
    public final String NAME;
    /**
     * Type of the topic used within the system.
     */
    public final DataType INPUT_TYPE;

    /**
     * Create a topic.
     *
     * @param name    Name of the topic to appear in Kafka.
     * @param type    Type of the topic used within the system.
     */
    public Topic(@Nonnull String name,
                 @Nonnull DataType type) {
        this.NAME = name;
        this.INPUT_TYPE = type;
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
