/*
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

package org.cripac.isee.pedestrian.reid;

import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.vpe.util.tracking.TrackletOrURL;

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

    public final TrackletOrURL trackletOrURL;
    public final Attributes attr;

    /**
     * The IDRANK of the pedestrian. -1 means not available.
     */
    public final int id;

    /**
     * Extracted feature for comparison, if applicable.
     */
    public final Feature feature;

    /**
     * Constructor with track and attributes specified at the beginning.
     *  @param trackletOrURL the tracklet of the pedestrian or the URL of the tracklet.
     * @param attr          attributes recognized from the pedestrian.
     * @param id
     */
    public PedestrianInfo(@Nullable TrackletOrURL trackletOrURL,
                          @Nullable Attributes attr,
                          int id) {
        this(trackletOrURL, attr, id, null);
    }

    /**
     * Constructor with track and attributes specified at the beginning.
     *
     * @param trackletOrURL the tracklet of the pedestrian or the URL of the tracklet.
     * @param attr          attributes recognized from the pedestrian.
     * @param feature
     */
    public PedestrianInfo(@Nullable TrackletOrURL trackletOrURL,
                          @Nullable Attributes attr,
                          @Nullable Feature feature) {
        this(trackletOrURL, attr, -1, feature);
    }

    /**
     * Constructor with track and attributes specified at the beginning.
     *
     * @param trackletOrURL the tracklet of the pedestrian or the URL of the tracklet.
     */
    public PedestrianInfo(@Nullable TrackletOrURL trackletOrURL) {
        this(trackletOrURL, null);
    }

    /**
     * Constructor with track and attributes specified at the beginning.
     *
     * @param trackletOrURL the tracklet of the pedestrian or the URL of the tracklet.
     * @param attr          attributes recognized from the pedestrian.
     */
    public PedestrianInfo(@Nullable TrackletOrURL trackletOrURL,
                          @Nullable Attributes attr) {
        this(trackletOrURL, attr, -1, null);
    }

    /**
     * Constructor with track and attributes specified at the beginning.
     *
     * @param trackletOrURL the tracklet of the pedestrian or the URL of the tracklet.
     * @param attr          attributes recognized from the pedestrian.
     */
    public PedestrianInfo(@Nullable TrackletOrURL trackletOrURL,
                          @Nullable Attributes attr,
                          int id,
                          @Nullable Feature feature) {
        this.trackletOrURL = trackletOrURL;
        this.attr = attr;
        this.id = id;
        this.feature = feature;
    }
}