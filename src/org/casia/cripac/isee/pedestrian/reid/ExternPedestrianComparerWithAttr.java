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
public class ExternPedestrianComparerWithAttr extends PedestrianComparerWithAttr {

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

		private void getBytesFromPedestrianInfo(PedestrianInfo pedestrian, BufferedOutputStream bufferedOutputStream)
				throws IOException {
			ByteBuffer byteBuffer;

			if (pedestrian.feature != null || enableFeatureOnly) {
				// 1 byte - 0: Full data; 1: Feature only
				bufferedOutputStream.write(1);

				// Feature.LENGTH bytes - Known feature.
				bufferedOutputStream.write(pedestrian.feature);
			} else {
				// 1 byte - 0: Full data; 1: Feature only
				bufferedOutputStream.write(0);

				// Track.
				Track track = pedestrian.track;
				// 4 bytes - Track length (number of bounding boxes).
				byteBuffer = ByteBuffer.allocate(Integer.BYTES);
				byteBuffer.putInt(track.locationSequence.length);
				bufferedOutputStream.write(byteBuffer.array());
				// Each bounding box.
				for (BoundingBox box : track.locationSequence) {
					// 16 bytes - Bounding box data.
					byteBuffer = ByteBuffer.allocate(Integer.BYTES * 4);
					byteBuffer.putInt(box.x);
					byteBuffer.putInt(box.y);
					byteBuffer.putInt(box.width);
					byteBuffer.putInt(box.height);
					bufferedOutputStream.write(byteBuffer.array());
					// width * height * 3 bytes - Image data.
					bufferedOutputStream.write(box.patchData);
				}

				// 4 * 2 bytes - Attributes.
				Attributes attr = pedestrian.attr;
				byteBuffer = ByteBuffer.allocate(Integer.BYTES * 2);
				byteBuffer.putInt(attr.sex);
				byteBuffer.putInt(attr.facing);
				bufferedOutputStream.write(byteBuffer.array());
			}

			bufferedOutputStream.flush();
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

	private class ResultLister implements Runnable {

		InputStream inputStream;

		/**
		 * Construct a listener listening to the socket.
		 * 
		 * @param inputStream
		 *            Input stream from the socket.
		 * @throws IOException
		 */
		public ResultLister(InputStream inputStream) {
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

			while (true) {
				try {
					inputStream.read(idMSBBuf);
					inputStream.read(idLSBBuf);
					int res = inputStream.read(similarityBuf);
					if (res == -1) {
						return;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
				UUID id = new UUID(ByteBuffer.wrap(idMSBBuf).order(ByteOrder.LITTLE_ENDIAN).getLong(),
						ByteBuffer.wrap(idLSBBuf).order(ByteOrder.LITTLE_ENDIAN).getLong());
				float similarity = ByteBuffer.wrap(similarityBuf).order(ByteOrder.LITTLE_ENDIAN).getFloat();

				synchronized (resultPool) {
					resultPool.put(id, similarity);
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
	public ExternPedestrianComparerWithAttr(InetAddress solverAddress, int port) throws IOException {
		socket = new Socket(solverAddress, port);
		resListeningThread = new Thread(new ResultLister(socket.getInputStream()));
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
	public ExternPedestrianComparerWithAttr(InetAddress solverAddress, int port, boolean enableFeatureOnly)
			throws IOException {
		this.enableFeatureOnly = enableFeatureOnly;

		socket = new Socket(solverAddress, port);
		resListeningThread = new Thread(new ResultLister(socket.getInputStream()));
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
		RequestMessage message = new RequestMessage(personA, personB);

		synchronized (socket) {
			message.getBytes(socket.getOutputStream());
		}

		while (true) {
			synchronized (resultPool) {
				if (resultPool.containsKey(message.id)) {
					return resultPool.get(message.id);
				} else {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
