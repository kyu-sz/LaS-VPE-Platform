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

package org.cripac.isee.vpe.util;

import java.io.*;

/**
 * The SerializationHelper class provides functions for serializing and
 * deserializing objects.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SerializationHelper {

    /**
     * Serialize an object.
     *
     * @param object The object to serialize.
     * @return A serialized byte array of the object.
     */
    public static byte[] serialize(Object object) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutput objectOutput = null;
        try {
            objectOutput = new ObjectOutputStream(byteArrayOutputStream);
            objectOutput.writeObject(object);

            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (objectOutput != null) {
                    objectOutput.close();
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

    /**
     * Deserialize a byte array of an object.
     *
     * @param byteArray The byte array serialized from an object.
     * @return An object from which the byte array is serialized.
     * @throws ClassNotFoundException On failure finding target class.
     */
    public static Serializable deserialize(byte[] byteArray) throws ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        ObjectInput objectInput = null;
        try {
            objectInput = new ObjectInputStream(byteArrayInputStream);
            return (Serializable) objectInput.readObject();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (objectInput != null) {
                    objectInput.close();
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
