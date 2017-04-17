/*
 * This file is part of LaS-VPE-Platform.
 *
 * LaS-VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE-Platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-4-17.
 */
package org.cripac.isee.alg.pedestrian.attr;

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;

import javax.annotation.Nonnull;

/**
 * Recognizers supporting batch recognizing should implement this interface.
 */
public interface BatchRecognizer {

    /**
     * Recognize attributes in parallel from a batch of pedestrian bounding boxes.
     *
     * @param boundingBoxes a batch of pedestrian bounding boxes.
     * @return attributes of the pedestrian in the batch.
     */
    @Nonnull
    Attributes[] recognize(@Nonnull Tracklet.BoundingBox[] boundingBoxes);
}
