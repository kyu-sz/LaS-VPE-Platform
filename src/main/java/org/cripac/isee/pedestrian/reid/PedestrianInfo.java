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

package org.cripac.isee.pedestrian.reid;

import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.tracking.Tracklet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * The class PedestrianInfo is a wrapper for any possible information of a
 * pedestrian. Currently, it may contain the track, attributes, IDRANK and feature
 * of a pedestrian. Information not available is marked null or -1.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianInfo implements Serializable, Cloneable {

    private static final long serialVersionUID = 5845993089063557863L;

    public Tracklet tracklet = null;
    public Attributes attr = null;

    /**
     * The IDRANK of the pedestrian. If not decided yet, it is left as -1.
     */
    public int id = -1;

    /**
     * Extracted feature for comparison, if applicable.
     */
    public Feature1024 feature = null;

    /**
     * Constructor with track and attributes specified at the beginning.
     *
     * @param tracklet The track of the pedestrian.
     * @param attr     Attributes recognized from the pedestrian.
     */
    public PedestrianInfo(@Nullable Tracklet tracklet,
                          @Nullable Attributes attr) {
        this.tracklet = tracklet;
        this.attr = attr;
    }

    /**
     * Empty constructor.
     */
    public PedestrianInfo() {
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "|" + tracklet + "|" + attr + "|";
    }
}