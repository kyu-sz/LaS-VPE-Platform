///*
// * This file is part of las-vpe-platform.
// *
// * las-vpe-platform is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * las-vpe-platform is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
// *
// * Created by ken.yu on 17-3-20.
// */
//package org.cripac.isee.alg;
//
//import org.apache.commons.io.IOUtils;
//import org.bytedeco.javacpp.Loader;
//import org.bytedeco.javacpp.opencv_core;
//import org.cripac.isee.vpe.util.logging.ConsoleLogger;
//import org.cripac.isee.vpe.util.logging.Logger;
//import org.tensorflow.Graph;
//import org.tensorflow.Session;
//
//import javax.annotation.Nonnull;
//import javax.annotation.Nullable;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//
//public class Tensorflow {
//
//    protected Logger logger;
//    protected Session session;
//    protected Graph graph;
//
//    /**
//     * Initialize Tensorflow with a frozen protobuf of a pre-trained model.
//     *  @param frozenPB      frozen protobuf of the pretrain-model.
//     *
//     */
//    protected void initialize(@Nonnull File frozenPB) throws IOException {
//        initialize(frozenPB, null);
//    }
//
//    /**
//     * Initialize Tensorflow with a frozen protobuf of a pre-trained model.
//     *
//     * @param frozenPB      frozen protobuf of the pretrain-model.
//     * @param sessionConfig configuration protobuf for session.
//     */
//    protected void initialize(@Nonnull File frozenPB,
//                              @Nullable File sessionConfig) throws IOException {
//        if (!frozenPB.exists()) {
//            throw new FileNotFoundException("Protobuf not found at " + frozenPB.getAbsolutePath());
//        }
//        logger.info("Loading Tensorflow frozen protobuf from " + frozenPB.getAbsolutePath());
//        byte[] sessionConfigBytes = sessionConfig == null ? null :
//                IOUtils.toByteArray(new FileInputStream(sessionConfig));
//        initialize(IOUtils.toByteArray(new FileInputStream(frozenPB)), sessionConfigBytes);
//    }
//
//    /**
//     * Initialize Tensorflow with a frozen protobuf of a pre-trained model.
//     *  @param frozenPB      frozen protobuf of the pretrain-model.
//     *
//     */
//    protected void initialize(@Nonnull byte[] frozenPB) throws IOException {
//        initialize(frozenPB, null);
//    }
//
//    /**
//     * Initialize Tensorflow with a frozen protobuf of a pre-trained model.
//     *
//     * @param frozenPB      frozen protobuf of the pretrain-model.
//     * @param sessionConfig serialized configuration protobuf for session.
//     */
//    protected void initialize(@Nonnull byte[] frozenPB,
//                              @Nullable byte[] sessionConfig) throws IOException {
//        graph = new Graph();
//        graph.importGraphDef(frozenPB);
//        session = new Session(graph, sessionConfig);
//        this.logger.debug("Tensorflow initialized!");
//    }
//
//    /**
//     * Create an instance of Tensorflow.
//     *
//     * @param gpu    index of GPU to use.
//     * @param logger logger for outputting debug info.
//     */
//    protected Tensorflow(String gpu,
//                         @Nullable Logger logger) {
//        Loader.load(opencv_core.class);
//
//        if (logger == null) {
//            this.logger = new ConsoleLogger();
//        } else {
//            this.logger = logger;
//        }
//    }
//}
