/***********************************************************************
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
 * along with LaS-VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.vpe.debug;

import java.util.Random;

import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.attr.Attributes.Facing;
import org.casia.cripac.isee.pedestrian.attr.Attributes.Sex;
import org.casia.cripac.isee.vpe.data.GraphDatabaseConnector;
import org.casia.cripac.isee.vpe.data.RecordUnavailableException;

/**
 * Simulate a database connector that provides tracks and attributes.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class FakeDatabaseConnector extends GraphDatabaseConnector {

	private Random rand = new Random();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.vpe.data.GraphDatabaseConnector#setTrackSavingPath(
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public void setTrackSavingPath(String id, String path) {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.vpe.data.GraphDatabaseConnector#getTrackSavingPath(
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public String getTrackSavingDir(String id) throws RecordUnavailableException {
		return "har:///user/labadmin/metadata/video123/8c617d98-340d-48c1-a388-f4b2499f4e9b.har";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.data.GraphDatabaseConnector#
	 * setPedestrianSimilarity(java.lang.String, java.lang.String, float)
	 */
	@Override
	public void setPedestrianSimilarity(String idA, String idB, float similarity) {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.data.GraphDatabaseConnector#
	 * getPedestrianSimilarity(java.lang.String, java.lang.String)
	 */
	@Override
	public float getPedestrianSimilarity(String idA, String idB) throws RecordUnavailableException {
		return rand.nextFloat();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.data.GraphDatabaseConnector#
	 * setPedestrianAttributes(java.lang.String,
	 * org.casia.cripac.isee.pedestrian.attr.Attributes)
	 */
	@Override
	public void setPedestrianAttributes(String id, Attributes attr) {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.data.GraphDatabaseConnector#
	 * getPedestrianAttributes(java.lang.String)
	 */
	@Override
	public Attributes getPedestrianAttributes(String id) throws RecordUnavailableException {
		Attributes attr = new Attributes();
		attr.facing = rand.nextInt(Facing.RIGHT);
		attr.sex = rand.nextInt(Sex.UNDETERMINED);
		return attr;
	}
}
