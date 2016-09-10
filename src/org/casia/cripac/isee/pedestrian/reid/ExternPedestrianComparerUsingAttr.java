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
package org.casia.cripac.isee.pedestrian.reid;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.pedestrian.tracking.Track.BoundingBox;

/**
 * ExternPedestrianComparerWithAttr is a pedestrian comparer using attributes
 * that depend on solvers extern to the LaS-VPE-Platform connected with sockets.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class ExternPedestrianComparerUsingAttr extends PedestrianComparerUsingAttr {

	/**
	 * The RequestMessage is a class specializing the format of request messages
	 * of ExternPedestrianComparerWithAttr to extern solvers.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	protected class RequestMessage implements Serializable {

		private static final long serialVersionUID = -2921106573399450286L;

		public UUID id = UUID.randomUUID();
		public PedestrianInfo personA = null;
		public PedestrianInfo personB = null;

		public RequestMessage(PedestrianInfo personA, PedestrianInfo personB) {
			this.personA = personA;
			this.personB = personB;
		}

		/**
		 * Get the byte array of a PedestrianInfo in a specified format then
		 * output to a stream.
		 * 
		 * @param pedestrianInfo
		 *            Information of a pedestrian.
		 * @param outputStream
		 *            The stream to output the byte array to.
		 * @throws IOException
		 */
		private void getBytesFromPedestrianInfo(PedestrianInfo pedestrianInfo, OutputStream outputStream)
				throws IOException {
			ByteBuffer byteBuffer;

			if (pedestrianInfo.feature != null || enableFeatureOnly) {
				// 1 byte - 0: Full data; 1: Feature only
				outputStream.write(1);

				// Feature.LENGTH bytes - Known feature.
				outputStream.write(pedestrianInfo.feature.vector);
			} else {
				// 1 byte - 0: Full data; 1: Feature only
				outputStream.write(0);

				// Track.
				Track track = pedestrianInfo.track;
				// 4 bytes - Track length (number of bounding boxes).
				byteBuffer = ByteBuffer.allocate(Integer.BYTES);
				byteBuffer.putInt(track.locationSequence.length);
				outputStream.write(byteBuffer.array());
				// Each bounding box.
				for (BoundingBox box : track.locationSequence) {
					// 16 bytes - Bounding box data.
					byteBuffer = ByteBuffer.allocate(Integer.BYTES * 4);
					byteBuffer.putInt(box.x);
					byteBuffer.putInt(box.y);
					byteBuffer.putInt(box.width);
					byteBuffer.putInt(box.height);
					outputStream.write(byteBuffer.array());
					// width * height * 3 bytes - Image data.
					outputStream.write(box.patchData);
				}

				// 4 * 2 bytes - Attributes.
				Attributes attr = pedestrianInfo.attr;
				byteBuffer = ByteBuffer.allocate(Integer.BYTES * 2);
				byteBuffer.putInt(attr.sex);
				byteBuffer.putInt(attr.facing);
				outputStream.write(byteBuffer.array());
			}

			outputStream.flush();
		}

		/**
		 * Given an output stream, the RequestMessage writes itself to the
		 * stream as a byte array in a specialized form.
		 * 
		 * @param outputStream
		 *            The output stream to write to.
		 * @throws IOException
		 */
		public void getBytes(OutputStream outputStream) throws IOException {
			BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

			// 16 bytes - Message ID.
			ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES);
			byteBuffer.putLong(id.getMostSignificantBits());
			byteBuffer.putLong(id.getLeastSignificantBits());
			bufferedOutputStream.write(byteBuffer.array());

			getBytesFromPedestrianInfo(personA, bufferedOutputStream);
			getBytesFromPedestrianInfo(personB, bufferedOutputStream);

			bufferedOutputStream.flush();
		}
	}

	/**
	 * The ResultListener class listens to the socket for comparison results
	 * then store them into the result pool.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	private class ResultListener implements Runnable {
		/**
		 * The input stream of the socket.
		 */
		InputStream inputStream;

		/**
		 * Construct a listener listening to the socket.
		 * 
		 * @param inputStream
		 *            Input stream from the socket.
		 * @throws IOException
		 */
		public ResultListener(InputStream inputStream) {
			this.inputStream = inputStream;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			byte[] idMSBBuf = new byte[8];
			byte[] idLSBBuf = new byte[8];
			byte[] similarityBuf = new byte[4];
			byte[] hasFeatVecBufA = new byte[1];
			byte[] featVecBufA = new byte[Feature.LENGTH];
			byte[] hasFeatVecBufB = new byte[1];
			byte[] featVecBufB = new byte[Feature.LENGTH];

			while (true) {
				// Receive data from socket.
				try {
					// 8 * 2 bytes - UUID of the task.
					inputStream.read(idMSBBuf);
					inputStream.read(idLSBBuf);
					// 4 bytes - Similarity.
					int res = inputStream.read(similarityBuf);
					if (res == -1) {
						return;
					}
					// 1 byte - Whether returning the feature vector of the
					// first pedestrian.
					inputStream.read(hasFeatVecBufA);
					// Feature.LENGTH bytes (Optional) - The feature vector of
					// the first pedestrian.
					inputStream.read(featVecBufA);
					// 1 byte - Whether returning the feature vector of the
					// second pedestrian.
					inputStream.read(hasFeatVecBufB);
					// Feature.LENGTH bytes (Optional) - The feature vector of
					// the second pedestrian.
					inputStream.read(featVecBufB);
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}

				// Parse the data into results.
				UUID id = new UUID(ByteBuffer.wrap(idMSBBuf).order(ByteOrder.LITTLE_ENDIAN).getLong(),
						ByteBuffer.wrap(idLSBBuf).order(ByteOrder.LITTLE_ENDIAN).getLong());
				float similarity = ByteBuffer.wrap(similarityBuf).order(ByteOrder.LITTLE_ENDIAN).getFloat();

				// Store the results.
				synchronized (resultPool) {
					resultPool.put(id, similarity);
				}
				if (hasFeatVecBufA[0] != 0) {
					// TODO Store the feature vector to somewhere.
				}
				if (hasFeatVecBufB[0] != 0) {
					// TODO Store the feature vector to somewhere.
				}
			}
		}
	}

	protected Socket socket;

	private Thread resListeningThread = null;
	private Map<UUID, Float> resultPool = new HashMap<>();
	private boolean enableFeatureOnly = true;

	/**
	 * Constructor of ExternPedestrianComparerWithAttr specifying extern
	 * solver's address and listening port.
	 * 
	 * @param solverAddress
	 *            The address of the solver.
	 * @param port
	 *            The port the solver is listening to.
	 * @throws IOException
	 */
	public ExternPedestrianComparerUsingAttr(InetAddress solverAddress, int port) throws IOException {
		socket = new Socket(solverAddress, port);
		resListeningThread = new Thread(new ResultListener(socket.getInputStream()));
		resListeningThread.start();
	}

	/**
	 * Constructor of ExternPedestrianComparerWithAttr specifying extern
	 * solver's address and listening port.
	 * 
	 * @param solverAddress
	 *            The address of the solver.
	 * @param port
	 *            The port the solver is listening to.
	 * @param enableFeatureOnly
	 *            Enable to compare pedestrians with feature only.
	 * @throws IOException
	 */
	public ExternPedestrianComparerUsingAttr(InetAddress solverAddress, int port, boolean enableFeatureOnly)
			throws IOException {
		this.enableFeatureOnly = enableFeatureOnly;

		socket = new Socket(solverAddress, port);
		resListeningThread = new Thread(new ResultListener(socket.getInputStream()));
		resListeningThread.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.pedestrian.reid.PedestrianComparerWithAttr#compare(
	 * org.casia.cripac.isee.pedestrian.reid.PedestrianInfo,
	 * org.casia.cripac.isee.pedestrian.reid.PedestrianInfo)
	 */
	@Override
	public float compare(PedestrianInfo personA, PedestrianInfo personB) throws IOException {
		// Create a new message consisting the comparation task.
		RequestMessage message = new RequestMessage(personA, personB);

		// Write the bytes of the message to the socket.
		synchronized (socket) {
			message.getBytes(socket.getOutputStream());
		}

		// Wait until the result is received and stored in the result pool.
		while (true) {
			synchronized (resultPool) {
				if (resultPool.containsKey(message.id)) {
					return resultPool.get(message.id);
				} else {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
