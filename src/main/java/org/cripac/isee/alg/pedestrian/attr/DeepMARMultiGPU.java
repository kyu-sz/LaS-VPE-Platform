package org.cripac.isee.alg.pedestrian.attr;/*
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
 * along with LaS-VPE-Platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-4-18.
 */

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;

public class DeepMARMultiGPU implements BatchRecognizer, Recognizer {
    private final @Nonnull
    Queue<Task> taskQueue = new ArrayDeque<>();
    private boolean toTerminate = false;
    private @Nonnull
    Executor[] executors;
    private @Nonnull
    Logger logger;
    private @Nullable
    File pb = null;
    private @Nullable
    File model = null;

    /**
     * Create an instance, specifying the GPUs this instance can use.
     *
     * @param gpus IDs of GPUs this instance can use, separated by commas.
     */
    public DeepMARMultiGPU(@Nonnull String gpus,
                           @Nonnull File pb,
                           @Nonnull File model,
                           @Nonnull Logger logger) {
        logger.info("Initializing DeepMARMultiGPU...");
        this.pb = pb;
        this.model = model;
        this.logger = logger;
        String[] gpuIDs = gpus.split(",");
        executors = new Executor[gpuIDs.length];
        initialize(gpuIDs);
    }

    /**
     * Create an instance, specifying the GPUs this instance can use.
     *
     * @param gpus IDs of GPUs this instance can use, separated by commas.
     */
    public DeepMARMultiGPU(@Nonnull String gpus,
                           @Nonnull Logger logger) {
        this.logger = logger;
        String[] gpuIDs = gpus.split(",");
        executors = new Executor[gpuIDs.length];
        initialize(gpuIDs);
    }

    private void initialize(@Nonnull String[] gpuIDs) {
        for (int i = 0; i < gpuIDs.length; ++i) {
            executors[i] = new Executor(gpuIDs[i]);
            executors[i].start();
        }

        new Thread(() -> {
            int roundRobin = 0;
            while (!toTerminate) {
                synchronized (taskQueue) {
                    while (taskQueue.isEmpty()) {
                        try {
                            taskQueue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    final int batchSize = Math.min(taskQueue.size(), 64);
                    final Task[] tasksInBatch = new Task[batchSize];
                    for (int i = 0; i < batchSize; ++i) {
                        tasksInBatch[i] = taskQueue.poll();
                        assert tasksInBatch[i] != null;
                    }
                    final Executor executor = executors[roundRobin++ % executors.length];
                    new Thread(() -> {
                        final Tracklet.BoundingBox[] boundingBoxes = new Tracklet.BoundingBox[batchSize];
                        for (int i = 0; i < batchSize; ++i) {
                            boundingBoxes[i] = tasksInBatch[i].bbox;
                        }
                        final Attributes[] results = executor.recognize(boundingBoxes);
                        for (int i = 0; i < batchSize; ++i) {
                            tasksInBatch[i].setResult(results[i]);
                        }
                    }).start();
                }
            }
        }).start();
    }

    @Override
    protected void finalize() throws Throwable {
        toTerminate = true;
        for (Executor executor : executors) {
            while (executor.isAlive()) {
                Thread.sleep(10);
            }
        }
        super.finalize();
    }

    /**
     * Recognize attributes in parallel from a batch of pedestrian bounding boxes.
     *
     * @param boundingBoxes a batch of pedestrian bounding boxes.
     * @return attributes of the pedestrian in the batch.
     */
    @Nonnull
    @Override
    public Attributes[] recognize(@Nonnull Tracklet.BoundingBox[] boundingBoxes) {
        TaskMonitor taskMonitor = new TaskMonitor(boundingBoxes.length);
        Task[] tasks = new Task[boundingBoxes.length];
        synchronized (taskQueue) {
            for (int i = 0; i < boundingBoxes.length; ++i) {
                tasks[i] = new Task(boundingBoxes[i], taskMonitor);
                taskQueue.add(tasks[i]);
            }
            taskQueue.notify();
        }
        taskMonitor.awaitAllTasks();
        Attributes[] results = new Attributes[boundingBoxes.length];
        for (int i = 0; i < boundingBoxes.length; ++i) {
            results[i] = tasks[i].result;
            assert results[i] != null;
        }
        return results;
    }

    /**
     * Recognize attributes from a pedestrian tracklet.
     *
     * @param tracklet a pedestrian tracklet.
     * @return attributes of the pedestrian specified by the tracklet.
     */
    @Nonnull
    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) {
        Collection<Tracklet.BoundingBox> samples = tracklet.getSamples();
        assert samples.size() >= 1;

        Tracklet.BoundingBox[] bboxes = new Tracklet.BoundingBox[samples.size()];
        samples.toArray(bboxes);
        Attributes[] results = recognize(bboxes);

        //noinspection OptionalGetWithoutIsPresent,ConstantConditions
        return Attributes.div(
                Arrays.stream(results)
                        .reduce(Attributes::add)
                        .get(),
                samples.size());
    }

    static class TaskMonitor {
        int tasksWaiting;

        TaskMonitor(int numTasks) {
            this.tasksWaiting = numTasks;
        }

        synchronized void anounceFinished() {
            if (--tasksWaiting == 0) {
                notify();
            }
        }

        synchronized void awaitAllTasks() {
            while (tasksWaiting > 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class Executor extends Thread {
        Tracklet.BoundingBox[] inputBuf = null;
        Attributes[] outputBuf = null;
        String gpu;

        Executor(@Nonnull String gpu) {
            this.gpu = gpu;
        }

        synchronized Attributes[] recognize(Tracklet.BoundingBox[] input) {
            assert inputBuf == null;
            assert outputBuf == null;
            inputBuf = input;
            notify();
            while (outputBuf == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Attributes[] results = outputBuf;
            outputBuf = null;
            return results;
        }

        @Override
        public void run() {
            final @Nonnull DeepMARCaffeNative deepMAR;
            try {
                if (pb == null || model == null) {
                    deepMAR = new DeepMARCaffeNative(gpu, logger);
                } else {
                    deepMAR = new DeepMARCaffeNative(gpu, pb, model, logger);
                }
            } catch (IOException e) {
                logger.error("Cannot initialize DeepMAR", e);
                return;
            }
            while (!toTerminate) {
                synchronized (this) {
                    while (inputBuf == null) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    assert inputBuf != null;
                    outputBuf = deepMAR.recognize(inputBuf);
                    inputBuf = null;
                    notify();
                }
            }
        }
    }

    class Task {
        final Tracklet.BoundingBox bbox;
        Attributes result = null;
        TaskMonitor monitor;

        Task(Tracklet.BoundingBox bbox, TaskMonitor monitor) {
            this.bbox = bbox;
            this.monitor = monitor;
        }

        void setResult(Attributes result) {
            this.result = result;
            monitor.anounceFinished();
        }
    }
}
