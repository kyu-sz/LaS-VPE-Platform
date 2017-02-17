/***********************************************************************
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
 ************************************************************************/

package org.cripac.isee.vpe.ctrl;

import com.google.gson.Gson;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.DataTypeNotMatchedException;
import org.cripac.isee.vpe.common.Topic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;

/**
 * The TaskData class contains a global execution plan and the execution result
 * of the predecessor node.
 *
 * @param <T> Type of the predecessor result this TaskData contains.
 */
public class TaskData<T extends Serializable> implements Serializable, Cloneable {

    private static final long serialVersionUID = 6817584209784831375L;
    /**
     * Current node to execute.
     */
    public ExecutionPlan.Node curNode;
    /**
     * Global execution plan.
     */
    public ExecutionPlan executionPlan = null;
    /**
     * Result of the predecessor.
     */
    public T predecessorRes = null;

    /**
     * Create an empty task.
     */
    public TaskData() {
        this.curNode = null;
        this.executionPlan = null;
    }

    /**
     * Create a task with an execution plan with no predecessor result.
     *
     * @param curNode       Current node to execute.
     * @param executionPlan A global execution plan.
     */
    public TaskData(@Nonnull ExecutionPlan.Node curNode,
                    @Nonnull ExecutionPlan executionPlan) {
        this.curNode = curNode;
        this.executionPlan = executionPlan;
    }

    /**
     * Create a task with an execution plan with predecessor result.
     *
     * @param curNode        Current node to execute.
     * @param executionPlan  A global execution plan.
     * @param predecessorRes Result of the predecessor node,
     *                       which is a serializable object.
     */
    public TaskData(@Nonnull ExecutionPlan.Node curNode,
                    @Nonnull ExecutionPlan executionPlan,
                    @Nonnull T predecessorRes) {
        this.curNode = curNode;
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
        private Hashtable<Integer, Node> nodes = new Hashtable<>();

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
         * Create a link from a head node to a tail node.
         * If the tail node has not been added to the execution plan,
         * it will be automatically added here with no execution data.
         *
         * @param headNode      The head node.
         * @param tailNodeTopic Input topic of the tail node.
         */
        public void letNodeOutputTo(@Nonnull Node headNode,
                                    @Nonnull Node tailNode,
                                    @Nonnull Topic tailNodeTopic) throws DataTypeNotMatchedException {
            if (headNode.outputType != tailNodeTopic.INPUT_TYPE) {
                throw new DataTypeNotMatchedException("Output type does not match with input type of topic "
                        + tailNodeTopic);
            }
            headNode.addSuccessor(tailNode, tailNodeTopic);
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

            /**
             * IO port of a node. Specified by a node and a particular topic.
             */
            public class Port implements Serializable {
                private static final long serialVersionUID = -762800114750459971L;

                public Node getNode() {
                    return Node.this;
                }

                public final Topic topic;

                private Port(Topic topic) {
                    this.topic = topic;
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
             *                 object.
             */
            private Node(int id, DataType outputType, Serializable execData) {
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

            Port getPort(Topic topic) {
                return new Port(topic);
            }

            /**
             * Add a node to the successor set of this node.
             *
             * @param topic An input of the node to add.
             */
            private void addSuccessor(Node node, Topic topic) {
                outputPorts.add(node.getPort(topic));
            }
        }
    }
}
