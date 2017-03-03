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

package org.cripac.isee.vpe.ctrl;

import com.google.gson.Gson;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

/**
 * The TaskData class contains a global execution plan and the execution result
 * of the predecessor node.
 */
public class TaskData implements Serializable, Cloneable {

    private static final long serialVersionUID = 6817584209784831375L;

    /**
     * Current nodes to execute.
     */
    public final Map<Stream.Port, ExecutionPlan.Node.Port> destPorts;

    /**
     * Global execution plan.
     */
    public final ExecutionPlan executionPlan;

    public final DataType outputType;

    /**
     * Result of the predecessor.
     */
    public final Serializable predecessorRes;

    /**
     * Get the destination node which contains the given port.
     *
     * @param port the port of the node which is one of the destination nodes in this {@link TaskData}
     * @return the destination node which contains the given port, or null
     * when the port is not in the destination ports.
     */
    @Nullable
    public ExecutionPlan.Node getDestNode(Stream.Port port) {
        return destPorts.get(port).getNode();
    }

    /**
     * Get the destination node which contains the given ports.
     *
     * @param possiblePorts a list of ports of the node which contains at least
     *                      one of the destination nodes in this {@link TaskData}
     * @return the destination node which contains the given ports, or null
     * when none of the possible ports are in the destination ports.
     */
    public ExecutionPlan.Node getDestNode(List<Stream.Port> possiblePorts) {
        for (Stream.Port port : possiblePorts) {
            final ExecutionPlan.Node node = getDestNode(port);
            if (node != null) {
                return node;
            }
        }
        return null;
    }

    /**
     * Create a task with an execution plan with no predecessor result.
     *
     * @param destPorts     Destination ports of this TaskData.
     * @param executionPlan A global execution plan.
     */
    public TaskData(@Nonnull Collection<ExecutionPlan.Node.Port> destPorts,
                    @Nonnull ExecutionPlan executionPlan) {
        this(destPorts, executionPlan, null);
    }

    /**
     * Create a task with an execution plan with predecessor result.
     *
     * @param destPort      Destination port of this TaskData.
     * @param executionPlan A global execution plan.
     */
    public TaskData(@Nonnull ExecutionPlan.Node.Port destPort,
                    @Nonnull ExecutionPlan executionPlan) {
        this(destPort, executionPlan, null);
    }

    /**
     * Create a task with an execution plan with predecessor result.
     *
     * @param destPort       Destination port of this TaskData.
     * @param executionPlan  A global execution plan.
     * @param predecessorRes Result of the predecessor node,
     *                       which is a serializable object.
     */
    public TaskData(@Nonnull ExecutionPlan.Node.Port destPort,
                    @Nonnull ExecutionPlan executionPlan,
                    @Nullable Serializable predecessorRes) {
        this(Collections.singleton(destPort), executionPlan, predecessorRes);
    }

    /**
     * Create a task with an execution plan with predecessor result.
     *
     * @param destPorts      Destination ports of this TaskData.
     * @param executionPlan  A global execution plan.
     * @param predecessorRes Result of the predecessor node,
     *                       which is a serializable object.
     */
    public TaskData(@Nonnull Collection<ExecutionPlan.Node.Port> destPorts,
                    @Nonnull ExecutionPlan executionPlan,
                    @Nullable Serializable predecessorRes) {
        this.destPorts = new Object2ObjectOpenHashMap<>();
        Optional<DataType> outputTypeOptional =
                destPorts.stream().map(port -> port.prototype.inputType).reduce((dataType1, dataType2) -> {
                    assert dataType1 == dataType2;
                    return dataType1;
                });
        assert outputTypeOptional.isPresent();
        outputType = outputTypeOptional.get();
        destPorts.forEach(port -> this.destPorts.put(port.prototype, port));
        this.executionPlan = executionPlan;
        this.predecessorRes = predecessorRes;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    /**
     * The ExecutionPlan class represents a directed acyclic graph of the
     * execution flows of modules. Each node is an execution of a module. Each
     * link represents that an execution should output to a next execution node.
     * One module may exist multiple times in a graph.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class ExecutionPlan implements Serializable, Cloneable {

        private static final long serialVersionUID = 1773757386865681468L;
        /**
         * Map for finding nodes according to the class of its module.
         */
        private Map<Integer, Node> nodes = new Object2ObjectOpenHashMap<>();

        private int nodeIDCounter = 0;

        /**
         * Combine two execution plans. If a node is marked executed in either
         * plan, the corresponding node in the combined plan is also marked
         * executed.
         *
         * @param planToCombine Another plan to combine on this plan.
         */
        public void combine(@Nonnull ExecutionPlan planToCombine) {
            planToCombine.nodes.values().stream()
                    .filter(Node::isExecuted)
                    .forEach(node -> this.nodes.get(node.id).markExecuted());
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return new Gson().toJson(this);
        }

        /**
         * Add a node to the execution plan. If it has been added previously with no
         * execution data, the new execution data will be added to the previous node.
         *
         * @param outputType Output data type of the node.
         * @return The new added node.
         */
        public Node addNode(@Nonnull DataType outputType) {
            return addNode(outputType, null);
        }

        /**
         * Add a node to the execution plan. If it has been added previously with no
         * execution data, the new execution data will be added to the previous node.
         *
         * @param outputType Output data type of the node.
         * @param execData   Data for execution of the node.
         * @return The new added node.
         */
        Node addNode(@Nonnull DataType outputType,
                     @Nullable Serializable execData) {
            Node node = new Node(nodeIDCounter++, outputType, execData);
            nodes.put(node.id, node);
            return node;
        }

        /**
         * Each node represents a flow of DStreams in an application.
         * Each node should produce only one kind of output.
         * Note that an application may contain more than one node.
         *
         * @author Ken Yu, CRIPAC, 2016
         */
        public class Node implements Serializable, Cloneable {

            private static final long serialVersionUID = 4538251384004287468L;

            private ExecutionPlan getPlan() {
                return ExecutionPlan.this;
            }

            public void outputTo(@Nonnull Port port) {
                assert this.getPlan() == port.getNode().getPlan();
                assert this.outputType == port.prototype.inputType;
                outputPorts.add(port);
            }

            /**
             * IO port of a node. Created from a execution node with a port topic.
             */
            public class Port implements Serializable {
                private static final long serialVersionUID = -762800114750459971L;

                public Node getNode() {
                    return Node.this;
                }

                public final Stream.Port prototype;

                public String getName() {
                    return prototype.name;
                }

                private Port(@Nonnull Stream.Port prototype) {
                    this.prototype = prototype;
                }
            }

            /**
             * Each node has its own successor nodes, each organized in a list.
             * The indexes of the set correspond to that of the nodes.
             */
            private List<Port> outputPorts = new ObjectArrayList<>();

            private final int id;
            final DataType outputType;

            /**
             * Marker recording whether the stream in this execution plan
             * has been executed.
             */
            private boolean executed = false;

            /**
             * The data for this execution.
             */
            private Serializable execData = null;

            /**
             * @param execData The data for execution, which is a serializable
             */
            private Node(int id,
                         @Nonnull DataType outputType,
                         @Nullable Serializable execData) {
                this.id = id;
                this.outputType = outputType;
                this.execData = execData;
            }

            /**
             * @return Successor nodes of this node.
             */
            public List<Port> getOutputPorts() {
                return outputPorts;
            }

            /**
             * @return Execution data of this node.
             */
            public Serializable getExecData() {
                return execData;
            }

            /**
             * @return Whether the node has been executed in this execution plan.
             */
            public boolean isExecuted() {
                return executed;
            }

            /**
             * Empty the data in the node for saving memory.
             */
            private void makeEmpty() {
                this.execData = null;
                this.outputPorts = null;
            }

            /**
             * Mark the stream as executed in the execution plan and
             * clear its data.
             */
            public void markExecuted() {
                if (!executed) {
                    executed = true;
                    makeEmpty();
                }
            }

            public Port createInputPort(Stream.Port prototype) {
                return new Port(prototype);
            }
        }
    }
}
