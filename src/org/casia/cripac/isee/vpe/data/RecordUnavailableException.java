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

/**
 * The class RecordUnavailableException is an exception that indicates
 * conditions that records to retrieve from the database are not available.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class RecordUnavailableException extends Exception {

	private static final long serialVersionUID = -2004231373174763893L;

	/**
	 * Create an empty exception.
	 */
	public RecordUnavailableException() {
	}

	/**
	 * Create an exception with message.
	 * 
	 * @param message
	 *            Exception message.
	 */
	public RecordUnavailableException(String message) {
		super(message);
	}

}
