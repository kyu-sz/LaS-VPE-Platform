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
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The ByteArrayFactory class provides functions for transformation between objects and byte arrays,
 * and byte array managing functions.
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class ByteArrayFactory {

	/**
	 * Append the length of a byte array to the head of it to produce a new byte array.
	 * The appended length occupies 4 bytes, so the new byte array is 4 bytes longer.
	 * This can be used for sending a byte array in a flow, so with the length information,
	 * this byte array can be split from other bytes.
	 * @param byteArray	The original byte array.
	 * @return			A new byte array whose heading 4 bytes representing
	 * 					the length of the original byte array,
	 * 					and the reset storing the original byte array.
	 */
	public static byte[] appendLengthToHead(byte[] byteArray) {
		int length = byteArray.length;
		
		byte[] combinedByteArray = new byte[length + 4];
		combinedByteArray[0] = (byte)((length >> 24) & 0xFF);
		combinedByteArray[1] = (byte)((length >> 16) & 0xFF);
		combinedByteArray[2] = (byte)((length >> 8) & 0xFF);
		combinedByteArray[3] = (byte)((length) & 0xFF);
		System.arraycopy(byteArray, 0, combinedByteArray, 4, length);
		
		return combinedByteArray;
	}
	
	/**
	 * The ByteStreamParts class wraps two byte arrays,
	 * which are expected to be two parts split from a byte stream.
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public static class ByteArrayQueueParts implements Serializable {
		private static final long serialVersionUID = -5572414689400366353L;
		public byte[] head;
		public byte[] rest;
	}
	/**
	 * Split a byte stream to a head part and the rest.
	 * It is expected that the first four bytes in the stream represent an integer,
	 * specifying the length of the head part. 
	 * @param byteStream	The long byte stream to split.
	 * @return				A wrapper for the two parts.
	 */
	public static ByteArrayQueueParts splitByteStream(byte[] byteStream) {
		
		int headLength = 0;
		for (int i = 0; i < 4; ++i) {
			headLength = (headLength << 8) | (byteStream[i] & 0xFF);
		}
		
		ByteArrayQueueParts parts = new ByteArrayQueueParts();
		parts.head = new byte[headLength];
		parts.rest = new byte[byteStream.length - headLength - 4];
		System.arraycopy(byteStream, 4, parts.head, 0, headLength);
		System.arraycopy(byteStream, headLength + 4, parts.rest, 0, parts.rest.length);
		
		return parts;
	}
	
	/**
	 * Combine two byte arrays into one byte array.
	 * It is strongly recommended that the two arrays should contain their length information in their head,
	 * so as they can be split out correctly.
	 * Use ObjectFactory.appendLengthToHead to add length information to the head.
	 * And use ObjectFactory.splitByteStream to split them out.
	 * @param head	The byte array to store in the head.
	 * @param tail	The byte array to store in the tail.
	 * @return		A combined byte array.
	 */
	public static byte[] combineByteArray(byte[] head, byte[] tail) {
		byte[] combinedByteArray = new byte[head.length + tail.length];
		System.arraycopy(head, 0, combinedByteArray, 0, head.length);
		System.arraycopy(tail, 0, combinedByteArray, head.length, tail.length);
		return combinedByteArray;
	}
	
	/**
	 * This method converts an object to a byte array.
	 * Call ObjectFactory.getObject(...) to recover.
	 * @param object	The object to convert.
	 * @return			A byte array containing the information of the object.
	 * @throws IOException
	 */
	public static byte[] getByteArray(Object object) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutput objectOutput = null;
		try {
			objectOutput = new ObjectOutputStream(byteArrayOutputStream);
			objectOutput.writeObject(object);
			
			return byteArrayOutputStream.toByteArray();
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
	 * Recover an object from a byte array.
	 * @param byteArray				The byte array containing the information of an object.
	 * @return						An object recovered with information stored in the byte array.
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static Object getObject(byte[] byteArray) throws ClassNotFoundException, IOException {
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
		ObjectInput objectInput = null;
		try {
			objectInput = new ObjectInputStream(byteArrayInputStream);
			return objectInput.readObject();
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
	
	/**
	 * Compress a byte array with GZIP.
	 * @param byteArray	The original byte array.
	 * @return			The compressed byte array.
	 * @throws IOException
	 */
	public static byte[] compress(byte[] byteArray) throws IOException {
		byte[] compressedByteArray = null;
		
		ByteArrayOutputStream byteArrayOutputStream = null;
		GZIPOutputStream gzipOutputStream = null;
		try {
			byteArrayOutputStream = new ByteArrayOutputStream();
			gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
			gzipOutputStream.write(byteArray);
			gzipOutputStream.finish();
			compressedByteArray = byteArrayOutputStream.toByteArray();
		} finally {
			if (gzipOutputStream != null) {
				gzipOutputStream.close();
			}
			if (byteArrayOutputStream != null) {
				byteArrayOutputStream.close();
			}
		}
		
		return compressedByteArray;
	}
	
	/**
	 * Decompress a byte array compressed with GZIP.
	 * @param compressedByteArray	A byte array compressed with GZIP.
	 * @return						The decompressed byte array.
	 * @throws IOException
	 */
	public static byte[] decompress(byte[] compressedByteArray) throws IOException {
		byte[] byteArray = null;
		
		ByteArrayInputStream byteArrayInputStream = null;
		GZIPInputStream gzipInputStream = null;
		ByteArrayOutputStream byteArrayOutputStream = null;
		try {
			byteArrayInputStream = new ByteArrayInputStream(compressedByteArray);
			gzipInputStream = new GZIPInputStream(byteArrayInputStream);
			byte[] buf = new byte[1024];
			int num = -1;
			byteArrayOutputStream = new ByteArrayOutputStream();
			while ((num = gzipInputStream.read(buf, 0, buf.length)) != -1) {
				byteArrayOutputStream.write(buf, 0, num);
			}
			byteArray = byteArrayOutputStream.toByteArray();
			byteArrayOutputStream.flush();
		} finally {
			if (byteArrayOutputStream != null) {
				byteArrayOutputStream.close();
			}
			if (gzipInputStream != null) {
				gzipInputStream.close();
			}
			if (byteArrayInputStream != null) {
				byteArrayInputStream.close();
			}
		}
		
		return byteArray;
	}
}
