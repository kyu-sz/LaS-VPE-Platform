/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.vpe.common;

import java.io.Serializable;

/**
 * <br>
 * The ObjectFactory class can produce objects in a fixed manner. It is usually
 * used in a combination with the ObjectSink class. </br>
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class ObjectFactory<T> implements Serializable {

	private static final long serialVersionUID = -7931301716101856732L;

	/**
	 * Produce a new object.
	 * 
	 * @return An object newly produced.
	 */
	public abstract T getObject();
}
