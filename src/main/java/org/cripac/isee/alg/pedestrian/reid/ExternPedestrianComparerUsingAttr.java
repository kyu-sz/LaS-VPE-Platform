/*
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
 */

package org.cripac.isee.alg.pedestrian.reid;

import com.google.gson.Gson;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * ExternPedestrianComparerWithAttr is a pedestrian comparer using attributes
 * that depend on external solvers connected with sockets. It sends comparison
 * requests each containing two package of pedestrian information to the solver,
 * then receives responses containing the similarity and feature vector
 * (optional). The requests and responses are processed asynchronously, matched
 * by request UUID.
 * <p>
 * <p>
 * </p>
 * <p>
 * Request format
 * </p>
 * <p>
 * 16 bytes - Request UUID.
 * </p>
 * <p>
 * foreach pedestrian (totally 2): {
 * </p>
 * <p>
 * 1 byte - 0: Full data; 1: Feature only (featFlag)
 * </p>
 * <p>
 * if featFlag: Feature.LENGTH bytes - Known feature.
 * </p>
 * <p>
 * else: {
 * </p>
 * <p>
 * 4 bytes - Tracklet length (number of bounding boxes).
 * </p>
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
 * 4 bytes - Length of JSON string representing attributes (jsonLen).
 * </p>
 * <p>
 * jsonLen bytes - UTF-8 JSON string representing attributes.
 * </p>
 * <p>
 * }
 * </p>
 * <p>
 * }
 * </p>
 * <p>
 * <p>
 * </p>
 * <p>
 * Response format
 * </p>
 * <p>
 * 16 bytes - Request UUID.
 * </p>
 * <p>
 * 4 bytes - Similarity.
 * </p>
 * <p>
 * foreach pedestrian (totally 2): {
 * </p>
 * <p>
 * 1 byte - Whether returning the feature vector (retFlag).
 * </p>
 * <p>
 * if retFlag: Feature.LENGTH bytes (Optional) - The feature vector of the
 * pedestrian.
 * </p>
 * <p>
 * }
 * </p>
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class ExternPedestrianComparerUsingAttr implements PedestrianComparerUsingAttr {

    private final Socket socket;
    private Thread resListeningThread = null;
    private final Map<UUID, Float> resultPool = new HashMap<>();
    private boolean enableFeatureOnly = true;

    /**
     * Constructor of ExternPedestrianComparerWithAttr specifying extern
     * solver's address and listening port.
     *
     * @param solverAddress the address of the solver.
     * @param port          the port the solver is listening to.
     * @throws IOException if an I/O error occurs when creating the socket.
     */
    public ExternPedestrianComparerUsingAttr(@Nonnull InetAddress solverAddress,
                                             int port) throws IOException {
        socket = new Socket(solverAddress, port);
        resListeningThread = new Thread(new ResultListener(socket.getInputStream()));
        resListeningThread.start();
    }

    /**
     * Constructor of ExternPedestrianComparerWithAttr specifying extern
     * solver's address and listening port.
     *
     * @param solverAddress     the address of the solver.
     * @param port              the port the solver is listening to.
     * @param enableFeatureOnly whether to enable comparing pedestrians with feature only.
     * @throws IOException if an I/O error occurs when creating the socket.
     */
    public ExternPedestrianComparerUsingAttr(@Nonnull InetAddress solverAddress,
                                             int port,
                                             boolean enableFeatureOnly)
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
     * PedestrianInfo,
     * PedestrianInfo)
     */
    @Override
    public float compare(@Nonnull PedestrianInfo personA,
                         @Nonnull PedestrianInfo personB) throws Exception {
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

    /**
     * The RequestMessage is a class specializing the format of request messages
     * of ExternPedestrianComparerWithAttr to extern solvers.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    protected class RequestMessage implements Serializable {

        private static final long serialVersionUID = -2921106573399450286L;

        public UUID id = UUID.randomUUID();
        PedestrianInfo personA = null;
        PedestrianInfo personB = null;

        public RequestMessage(@Nonnull PedestrianInfo personA,
                              @Nonnull PedestrianInfo personB) {
            this.personA = personA;
            this.personB = personB;
        }

        /**
         * Get the byte array of a PedestrianInfo in a specified format then
         * output to a stream.
         *
         * @param pedestrianInfo Information of a pedestrian.
         * @param outputStream   The stream to output the byte array to.
         * @throws Exception on failure of getting tracklet from HDFS.
         */
        private void getBytesFromPedestrianInfo(@Nonnull PedestrianInfo pedestrianInfo,
                                                @Nonnull OutputStream outputStream)
                throws Exception {
            ByteBuffer byteBuffer;

            if (pedestrianInfo.feature != null && enableFeatureOnly) {
                // 1 byte - 0: Full data; 1: Feature only
                outputStream.write(1);

                // Feature.LENGTH bytes - Known feature.
                outputStream.write(pedestrianInfo.feature.getBytes());
            } else {
                // 1 byte - 0: Full data; 1: Feature only
                outputStream.write(0);

                // Tracklet samples.
                Tracklet tracklet = pedestrianInfo.trackletOrURL.getTracklet();
                Collection<Tracklet.BoundingBox> samples = tracklet.getSamples();
                // 4 bytes - number of samples in the tracklet.
                byteBuffer = ByteBuffer.allocate(Integer.BYTES);
                byteBuffer.putInt(samples.size());
                outputStream.write(byteBuffer.array());
                // Each bounding box.
                for (Tracklet.BoundingBox bbox : samples) {
                    // 16 bytes - Bounding box data.
                    // width * height * 3 bytes - Image data.
                    outputStream.write(bbox.toBytes());
                }

                // Attributes.
                Attributes attr = pedestrianInfo.attr;
                String attrJson = new Gson().toJson(attr);
                // 4 bytes - Length of JSON string representing Attributes
                // (jsonLen).
                byteBuffer = ByteBuffer.allocate(Integer.BYTES);
                byteBuffer.putInt(attrJson.length());
                outputStream.write(byteBuffer.array());
                // jsonLen bytes - UTF-8 JSON string representing attributes.
                outputStream.write(attrJson.getBytes(StandardCharsets.UTF_8));
            }

            outputStream.flush();
        }

        /**
         * Given an output stream, the RequestMessage writes itself to the
         * stream as a byte array in a specialized form.
         *
         * @param outputStream The output stream to write to.
         * @throws Exception on failure of getting tracklet from HDFS.
         */
        void getBytes(@Nonnull OutputStream outputStream) throws Exception {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

            // 16 bytes - Request UUID.
            ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            byteBuffer.putLong(id.getMostSignificantBits());
            byteBuffer.putLong(id.getLeastSignificantBits());
            bufferedOutputStream.write(byteBuffer.array());

            // Get bytes from each of the pedestrians respectively.
            getBytesFromPedestrianInfo(personA, bufferedOutputStream);
            getBytesFromPedestrianInfo(personB, bufferedOutputStream);

            // Flush the bytes to the socket.
            bufferedOutputStream.flush();
        }
    }

    /**
     * The ResultListener class listens to the socket for comparison results
     * then store them into the result pool.
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
         */
        ResultListener(@Nonnull InputStream inputStream) {
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
            byte[] featVecBufA = new byte[Feature1024.NUM_BYTES];
            byte[] hasFeatVecBufB = new byte[1];
            byte[] featVecBufB = new byte[Feature1024.NUM_BYTES];

            while (true) {
                // Receive data from socket.
                try {
                    int ret;
                    // 8 * 2 bytes - Request UUID.
                    ret = inputStream.read(idMSBBuf, 0, idMSBBuf.length);
                    assert idMSBBuf.length == ret;
                    ret = inputStream.read(idLSBBuf, 0, idLSBBuf.length);
                    assert idLSBBuf.length == ret;
                    // 4 bytes - Similarity.
                    ret = inputStream.read(similarityBuf, 0, similarityBuf.length);
                    assert similarityBuf.length == ret;
                    // 1 byte - Whether returning the feature vector of the
                    // first pedestrian.
                    ret = inputStream.read(hasFeatVecBufA);
                    assert ret == 1;
                    if (hasFeatVecBufA[0] != 0) {
                        // Feature.LENGTH bytes (Optional) - The feature vector
                        // of the first pedestrian.
                        ret = inputStream.read(featVecBufA, 0, featVecBufA.length);
                        assert (featVecBufA.length == ret);
                    }
                    // 1 byte - Whether returning the feature vector of the
                    // second pedestrian.
                    ret = inputStream.read(hasFeatVecBufB);
                    assert ret == 1;
                    if (hasFeatVecBufB[0] != 0) {
                        // Feature.LENGTH bytes (Optional) - The feature vector
                        // of the second pedestrian.
                        ret = inputStream.read(featVecBufB, 0, featVecBufB.length);
                        assert (featVecBufB.length == ret);
                    }
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
}
