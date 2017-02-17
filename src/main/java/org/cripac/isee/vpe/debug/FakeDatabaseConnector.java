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

package org.cripac.isee.vpe.debug;

import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Simulate a database connector that provides tracklets and attributes.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class FakeDatabaseConnector extends GraphDatabaseConnector {

    private Random rand = new Random();

    /*
     * (non-Javadoc)
     *
     * @see
     * GraphDatabaseConnector#setTrackSavingPath(
     * java.lang.String, java.lang.String)
     */
    @Override
    public void setTrackSavingPath(@Nonnull String id,
                                   @Nonnull String path) {
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * GraphDatabaseConnector#getTrackSavingPath(
     * java.lang.String, java.lang.String)
     */
    @Override
    public String getTrackletSavingDir(@Nonnull String id) throws NoSuchElementException {
        return "har:///user/labadmin/metadata/" +
                "pedestrian-tracking-isee-basic-CAM01_0/" +
                "fb7284a6-3064-4a49-aa93-a701368eec7b.har";
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * setPedestrianSimilarity(java.lang.String, java.lang.String, float)
     */
    @Override
    public void setPedestrianSimilarity(@Nonnull String idA,
                                        @Nonnull String idB, float similarity) {
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * getPedestrianSimilarity(java.lang.String, java.lang.String)
     */
    @Override
    public float getPedestrianSimilarity(@Nonnull String idA,
                                         @Nonnull String idB) throws NoSuchElementException {
        return rand.nextFloat();
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * setPedestrianAttributes(java.lang.String,
     * Attributes)
     */
    @Override
    public void setPedestrianAttributes(@Nonnull String id,
                                        @Nonnull Attributes attr) {
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * getPedestrianAttributes(java.lang.String)
     */
    @Override
    public Attributes getPedestrianAttributes(@Nonnull String id) throws NoSuchElementException {
        return new Attributes();
    }

}
