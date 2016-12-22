/***************************************************************************
 *  This file is part of LaS-VPE Platform.
 *
 *  LaS-VPE Platform is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  LaS-VPE Platform is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 ***************************************************************************/

/*
 * ExternPedestrianAttrRecognizer.java
 *
 *  Created on: Sep 22, 2016
 *      Author: ken
 */

package org.cripac.isee.pedestrian.attr;

import com.google.gson.Gson;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.pedestrian.tracking.Tracklet.BoundingBox;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The class ExternPedestrianAttrRecognizer is a recognizer of pedestrian
 * attributes that depend on external solvers connected with sockets. It sends
 * recognition requests each containing a track to the solver, then receives
 * responses containing the attributes. The requests and responses are processed
 * asynchronously, matched by request UUID.
 * Request format
 * </p>
 * 16 bytes - Request UUID.
 * </ p>
 * 4 bytes - Tracklet length (number of bounding boxes).
 * </ p>
 * <p>
 * foreach bounding box: {
 * </p>
 * <p>
 * 16 bytes - Bounding box data (x, y, width, height).
 * </p>
 * <p>
 * width * height * 3 bytes - Image data.
 * </p>
 * <p>
 * }
 * </p>
 * <p>
 * </p>
 * <p>
 * Response format
 * </p>
 * <p>
 * 16 bytes - Request UUID.
 * </p>
 * <p>
 * 4 bytes - Length of JSON representing the attributes (jsonLen).
 * </p>
 * <p>
 * jsonLen bytes - Bytes of UTF-8 JSON string representing the attributes.
 * </p>
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class ExternPedestrianAttrRecognizer extends PedestrianAttrRecognizer {

    protected Socket socket;
    private Thread resListeningThread = null;
    private Map<UUID, Attributes> resultPool = new HashMap<>();
    private Logger logger;

    /**
     * Constructor of ExternPedestrianAttrRecognizer specifying extern solver's
     * address and listening port.
     *
     * @param solverAddress The address of the solver.
     * @param port          The port the solver is listening to.
     * @throws IOException
     */
    public ExternPedestrianAttrRecognizer(@Nonnull InetAddress solverAddress,
                                          int port,
                                          @Nullable Logger logger) throws IOException {
        socket = new Socket(solverAddress, port);
        resListeningThread = new Thread(new ResultListener(socket.getInputStream()));
        resListeningThread.start();
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * PedestrianAttrRecognizer#recognize(
     * Tracklet)
     */
    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) throws IOException {
        // Create a new message consisting the comparation task.
        RequestMessage message = new RequestMessage(tracklet);

        // Write the bytes of the message to the socket.
        synchronized (socket) {
            message.getBytes(socket.getOutputStream());
        }
        logger.debug("Sent request " + message.id + " for tracklet " + tracklet.id);

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

    /**
     * The RequestMessage is a class specializing the format of request messages
     * of ExternPedestrianComparerWithAttr to extern solvers.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    protected class RequestMessage implements Serializable {

        private static final long serialVersionUID = -2921106573399450286L;

        public UUID id = UUID.randomUUID();
        public Tracklet tracklet = null;

        /**
         * Create a message requesting attribute recognition on a track.
         *
         * @param tracklet The track to recognize attributes from.
         */
        public RequestMessage(@Nonnull Tracklet tracklet) {
            this.tracklet = tracklet;
        }

        /**
         * Given an output stream, the RequestMessage writes itself to the
         * stream as a byte array in a specialized form.
         *
         * @param outputStream The output stream to write to.
         * @throws IOException
         */
        public void getBytes(@Nonnull OutputStream outputStream) throws IOException {
            BufferedOutputStream bufferedStream = new BufferedOutputStream(outputStream);

            // 16 bytes - Request UUID.
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
            buf.putLong(id.getMostSignificantBits());
            buf.putLong(id.getLeastSignificantBits());
            bufferedStream.write(buf.array());

            // 4 bytes - Tracklet length (number of bounding boxes).
            buf = ByteBuffer.allocate(Integer.BYTES);
            buf.putInt(tracklet.locationSequence.length);
            bufferedStream.write(buf.array());
            // Each bounding box.
            for (BoundingBox bbox : tracklet.locationSequence) {
                // 16 bytes - Bounding box data.
                buf = ByteBuffer.allocate(Integer.BYTES * 4);
                buf.putInt(bbox.x);
                buf.putInt(bbox.y);
                buf.putInt(bbox.width);
                buf.putInt(bbox.height);
                bufferedStream.write(buf.array());
                // width * height * 3 bytes - Image data.
                bufferedStream.write(bbox.patchData);
            }

            bufferedStream.flush();
        }
    }

    /**
     * The ResultListener class listens to the socket for pedestrian attributes
     * passed in JSON format then store them into the result pool.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    private class ResultListener implements Runnable {
        /**
         * The input stream of the socket.
         */
        InputStream inputStream;

        /**
         * Construct a listener listening to the socket.
         *
         * @param inputStream Input stream from the socket.
         * @throws IOException
         */
        public ResultListener(@Nonnull InputStream inputStream) {
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
            byte[] jsonLenBytes = new byte[4];
            byte[] jsonBytes = null;

            while (true) {
                UUID id;

                // Receive data from socket.
                logger.debug("Result listener: Starting a new round of message receiving.");
                try {
                    int ret = 0;
                    // 8 * 2 bytes - Request UUID.
                    ret = inputStream.read(idMSBBuf, 0, idMSBBuf.length);
                    assert (idMSBBuf.length == ret);
                    ret = inputStream.read(idLSBBuf, 0, idLSBBuf.length);
                    assert (idLSBBuf.length == ret);
                    id = new UUID(ByteBuffer.wrap(idMSBBuf).getLong(), ByteBuffer.wrap(idLSBBuf).getLong());
                    logger.debug("Result listener: Receiving result for request " + id + ".");

                    // 4 bytes - Length of JSON.
                    ret = inputStream.read(jsonLenBytes, 0, jsonLenBytes.length);
                    assert (jsonLenBytes.length == ret);

                    int jsonLen = ByteBuffer.wrap(jsonLenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
                    // Create a buffer for JSON.
                    jsonBytes = new byte[jsonLen];
                    logger.debug("Result listener: To receive " + jsonLen + " bytes.");

                    // jsonLen bytes - Bytes of UTF-8 JSON string representing
                    // the attributes.
                    ret = inputStream.read(jsonBytes, 0, jsonBytes.length);
                    assert (jsonBytes.length == ret);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                // Parse the data into results.
                Attributes attr = new Gson().fromJson(new String(jsonBytes, StandardCharsets.UTF_8), Attributes.class);
                logger.debug("Result listener: Result for request " + id + " is " + attr);

                // Store the results.
                synchronized (resultPool) {
                    resultPool.put(id, attr);
                }
                logger.debug("Result listener: Result for request " + id + " is stored.");
            }
        }
    }

}
