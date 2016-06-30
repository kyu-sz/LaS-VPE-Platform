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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * @author Ken Yu, ISEE, 2016
 *
 */
public class ObjectFactory {
	public static byte[] getByteArray(Object object) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutput output = null;
		try {
			output = new ObjectOutputStream(byteArrayOutputStream);
			output.writeObject(object);
			return byteArrayOutputStream.toByteArray();
		} finally {
			try {
				if (output != null) {
					output.close();
				}
			} catch (IOException e) {
				// ignore close exception
			}
			try {
				byteArrayOutputStream.close();
			} catch (IOException e) {
				// ignore close exception
			}
		}
	}
	
	public static Object getObject(byte[] byteArray) throws ClassNotFoundException, IOException {
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
		ObjectInput input = null;
		try {
			input = new ObjectInputStream(byteArrayInputStream);
			return input.readObject();
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (IOException e) {
				// ignore close exception
			}
			try {
				byteArrayInputStream.close();
			} catch (IOException e) {
				// ignore close exception
			}
		}
	}
}
