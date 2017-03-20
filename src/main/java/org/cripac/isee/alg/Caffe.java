package org.cripac.isee.alg;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.jcublas.JCublas;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.caffe;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;

import static org.bytedeco.javacpp.caffe.TEST;
import static org.bytedeco.javacpp.caffe.caffe_cpu_gemm_float;

/**
 * Base class of classes using Caffe.
 * Created by Ken Yu on 2017/3/7.
 */
public class Caffe {
    /**
     * Instance of Caffe.
     */
    protected caffe.FloatNet net = null;

    protected Logger logger;

    /**
     * Initialize Caffe with protocol and pre-trained model.
     *
     * @param protobuf Caffe protocol buffer file.
     * @param model    Caffe model file.
     */
    protected void initialize(@Nonnull File protobuf,
                              @Nonnull File model) throws FileNotFoundException {
        if (!protobuf.exists()) {
            throw new FileNotFoundException("Cannot find Caffe protocol from " + protobuf.getAbsolutePath());
        }
        logger.info("Loading Caffe protocol from " + protobuf.getAbsolutePath());
        net = new caffe.FloatNet(protobuf.getAbsolutePath(), TEST);

        if (!model.exists()) {
            throw new FileNotFoundException("Cannot find Caffe model from " + model.getAbsolutePath());
        }
        logger.info("Loading Caffe weights from " + model.getAbsolutePath());
        net.CopyTrainedLayersFrom(model.getAbsolutePath());

        this.logger.debug("Caffe initialized!");
    }

    /**
     * Create an instance of DeepMAR.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    protected Caffe(int gpu,
                    @Nullable Logger logger) {
        Loader.load(opencv_core.class);
        Loader.load(caffe.class);

        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

        testCuBLAS(gpu);

        if (gpu >= 0) {
            this.logger.info("Use GPU with device ID " + gpu);
            caffe.Caffe.SetDevice(gpu);
            caffe.Caffe.set_mode(caffe.Caffe.GPU);
        } else {
            this.logger.info("Use CPU.");
            caffe.Caffe.set_mode(caffe.Caffe.CPU);
        }
        this.logger.debug("Caffe mode and device set!");
    }

    private void testCuBLAS(int gpu) {
        final int N = 275;
        if (gpu >= 0) {
            logger.info("Starting CuBLAS test!");

            float h_A[];
            float h_B[];
            float h_C[];
            Pointer d_A = new Pointer();
            Pointer d_B = new Pointer();
            Pointer d_C = new Pointer();
            float alpha = 1.0f;
            float beta = 0.0f;
            int n2 = N * N;

        /* Initialize JCublas */
            JCublas.cublasInit();

            logger.info("CuBLAS initialized!");

        /* Allocate host memory for the matrices */
            h_A = new float[n2];
            h_B = new float[n2];
            h_C = new float[n2];

        /* Fill the matrices with test data */
            for (int i = 0; i < n2; i++) {
                h_A[i] = (float) Math.random();
                h_B[i] = (float) Math.random();
                h_C[i] = (float) Math.random();
            }

        /* Allocate device memory for the matrices */
            JCublas.cublasAlloc(n2, Sizeof.FLOAT, d_A);
            JCublas.cublasAlloc(n2, Sizeof.FLOAT, d_B);
            JCublas.cublasAlloc(n2, Sizeof.FLOAT, d_C);

            logger.info("Device memory allocated!");

        /* Initialize the device matrices with the host matrices */
            JCublas.cublasSetVector(n2, Sizeof.FLOAT, Pointer.to(h_A), 1, d_A, 1);
            JCublas.cublasSetVector(n2, Sizeof.FLOAT, Pointer.to(h_B), 1, d_B, 1);
            JCublas.cublasSetVector(n2, Sizeof.FLOAT, Pointer.to(h_C), 1, d_C, 1);

            logger.info("Device data set!");

        /* Performs operation using JCublas */
            JCublas.cublasSgemm('n', 'n', N, N, N, alpha,
                    d_A, N, d_B, N, beta, d_C, N);

            logger.info("SGEMM performed!");

        /* Read the result back */
            JCublas.cublasGetVector(n2, Sizeof.FLOAT, d_C, 1, Pointer.to(h_C), 1);

            logger.info("Results retrieved!");

        /* Memory clean up */

        /* Check result */
            float[] res = h_C.clone();
            caffe_cpu_gemm_float(111, 111, N, N, N, alpha, h_A, h_B, beta, res);
            for (int i = 0; i < n2; ++i) {
                if (Math.abs(h_C[i] - res[i]) > 1e-5) {
                    logger.error("Result different at i: " + h_C[i] + " vs " + res[i]);
                    break;
                }
            }

            JCublas.cublasFree(d_A);
            JCublas.cublasFree(d_B);
            JCublas.cublasFree(d_C);

            logger.info("Device memory freed!");

        /* Shutdown */
            JCublas.cublasShutdown();

            logger.info("CuBLAS test finished!");
        }
    }
}
