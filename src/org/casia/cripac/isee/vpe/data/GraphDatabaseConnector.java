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
package org.casia.cripac.isee.vpe.data;

import org.casia.cripac.isee.pedestrian.attr.Attributes;

/**
 * The class GraphDatabaseConnector is the base class for connector to graph
 * databases.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class GraphDatabaseConnector {

	/**
	 * Set the path of the directory saving the track of a pedestrian.
	 * 
	 * @param id
	 *            The ID of the pedestrian.
	 * @param path
	 *            The path of the directory saving the track of the pedestrian.
	 */
	public abstract void setTrackSavingPath(String id, String path);

	/**
	 * Get the path of the directory saving the track of a pedestrian.
	 * 
	 * @param id
	 *            The ID of the pedestrian.
	 * @return The path of the directory saving the track of the pedestrian.
	 * @throws RecordUnavailableException
	 */
	public abstract String getTrackSavingDir(String id) throws RecordUnavailableException;

	/**
	 * Set the similarity between two pedestrians.
	 * 
	 * @param idA
	 *            The ID of the first pedestrian.
	 * @param idB
	 *            The ID of the second pedestrian.
	 * @param similarity
	 *            The similarity between them.
	 */
	public abstract void setPedestrianSimilarity(String idA, String idB, float similarity);

	/**
	 * Get the similarity between two pedestrians.
	 * 
	 * @param idA
	 *            The ID of the first pedestrian.
	 * @param idB
	 *            The ID of the second pedestrian.
	 * @return The similarity between them.
	 */
	public abstract float getPedestrianSimilarity(String idA, String idB) throws RecordUnavailableException;

	/**
	 * Set the attributes of a pedestrian.
	 * 
	 * @param id
	 *            The ID of the pedestrian.
	 * @param attr
	 *            The attributes of the pedestrian.
	 */
	public abstract void setPedestrianAttributes(String id, Attributes attr);

	/**
	 * Get the attributes of a pedestrian.
	 * 
	 * @param id
	 *            The ID of the pedestrian.
	 * @return The attributes of the pedestrian.
	 */
	public abstract Attributes getPedestrianAttributes(String id) throws RecordUnavailableException;
}
