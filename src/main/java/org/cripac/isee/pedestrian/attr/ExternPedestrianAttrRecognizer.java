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

/**
 * The class ExternPedestrianAttrRecognizer is a recognizer of pedestrian
 * attributes that depend on external solvers connected with sockets. It sends
 * recognition requests each containing a track to the solver, then receives
 * responses containing the attributes. The requests and responses are processed
 * asynchronously, matched by request UUID.
 * Request format
 * <p>
 * 16 bytes - Request UUID.
 * <p>
 * 4 bytes - Tracklet length (number of bounding boxes).
 * <p>
 * foreach bounding box: {
 * <p>
 * 16 bytes - Bounding box data (x, y, width, height).
 * <p>
 * width * height * 3 bytes - Image data.
 * <p>
 * }
 * <p>
 * Response format
 * <p>
 * jsonLen bytes - Bytes of JSON string representing the attributes.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class ExternPedestrianAttrRecognizer extends PedestrianAttrRecognizer {

    private Logger logger;
    private Socket socket;
    private InetAddress solverAddress;
    private int port;

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
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }
        logger.debug("Using extern recognition server at " + solverAddress.getHostAddress() + ":" + port);
        connect(solverAddress, port);
    }

    public void connect(@Nonnull InetAddress solverAddress, int port) {
        this.solverAddress = solverAddress;
        this.port = port;
        connect();
    }

    private void connect() {
        while (true) {
            try {
                socket = new Socket(solverAddress, port);
                break;
            } catch (IOException e) {
                logger.error("When connecting to extern attr recog server", e);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
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
    public Attributes recognize(@Nonnull Tracklet tracklet) {
        // Create a new message consisting the comparation task.
        RequestMessage message = new RequestMessage(tracklet);

        // Write the bytes of the message to the socket.
        while (true) {
            try {
                message.getBytes(socket.getOutputStream());
                logger.debug("Sent request for tracklet " + tracklet.id);

                // Receive data from socket.
                logger.debug("Starting to receive messages.");
                return new Gson().fromJson(new InputStreamReader(socket.getInputStream()), Attributes.class);
            } catch (IOException e) {
                logger.error("When communicating with extern attr recog server", e);
                connect();
                logger.info("Connection recovered!");
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

            // 4 bytes - Tracklet length (number of bounding boxes).
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
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
}
