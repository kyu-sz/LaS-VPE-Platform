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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.cripac.isee.vpe.common.DataTypeNotMatchedException;
import org.cripac.isee.vpe.common.RecordNotFoundException;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.common.Topic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The TaskData class contains a global execution plan and the execution result
 * of the predecessor node.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class TaskData implements Serializable, Cloneable {

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
    public Serializable predecessorRes = null;
    /**
     * Information of the predecessor stream.
     */
    public Stream.Info predecessorInfo = null;

    /**
     * Change the current node to be executed.
     * The node is specified by one of its topic.
     *
     * @param topic A topic of the new current node.
     */
    public void changeCurNode(@Nonnull Topic topic)
            throws RecordNotFoundException {
        predecessorInfo = curNode.streamInfo;
        curNode = executionPlan.findNode(topic);
    }

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
                    @Nonnull Serializable predecessorRes) {
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
        return "|TaskData-node=" + curNode.getStreamInfo()
                + "-ExecutionPlan=" + executionPlan + "-PredecessorRes=\n"
                + predecessorRes + "|";
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
        private Map<Stream.Info, Node> nodes = new Object2ObjectOpenHashMap<>();

        /**
         * Find a node in the execution plan by topic.
         *
         * @param topic A topic of the node.
         * @return The node possessing the topic.
         */
        public Node findNode(@Nonnull Topic topic)
                throws RecordNotFoundException {
            if (topic.STREAM_INFO == null) {
                throw new RecordNotFoundException("The topic " + topic
                        + " has no stream information!");
            } else {
                return findNode(topic.STREAM_INFO);
            }
        }

        /**
         * Find a node in the execution plan by stream's information.
         *
         * @param info Information of the stream.
         * @return The node representing the stream.
         */
        public Node findNode(@Nonnull Stream.Info info)
                throws RecordNotFoundException {
            if (!nodes.containsKey(info)) {
                throw new RecordNotFoundException(info
                        + " cannot be found in execution plan!"
                        + " Available streams are: "
                        + nodes.keySet().stream()
                        .map(Stream.Info::toString)
                        .reduce("", (s1, s2) -> s1 + " " + s2));
            }
            return nodes.get(info);
        }

        /**
         * Combine two execution plans. If a node is marked executed in either
         * plan, the corresponding node in the combined plan is also marked
         * executed. Otherwise, nodes or execution data of nodes existing in
         * either plan will appear in the execution plan.
         *
         * @param a A plan.
         * @param b Another plan.
         * @return A combined execution plan.
         */
        public static ExecutionPlan combine(@Nonnull ExecutionPlan a,
                                            @Nonnull ExecutionPlan b) {
            ExecutionPlan combined = new ExecutionPlan();

            for (Stream.Info info : a.nodes.keySet()) {
                combined.addNode(info, a.nodes.get(info));
            }
            for (Stream.Info info : b.nodes.keySet()) {
                if (!combined.nodes.containsKey(info)) {
                    combined.addNode(info, b.nodes.get(info));
                } else {
                    if (combined.nodes.get(info).execData == null
                            && b.nodes.get(info).execData != null) {
                        combined.nodes.get(info).execData = b.nodes.get(info).execData;
                    }
                }
            }

            for (Stream.Info info : combined.nodes.keySet()) {
                if (a.nodes.get(info).executed || b.nodes.get(info).executed) {
                    combined.nodes.get(info).markExecuted();
                }
            }

            return combined;
        }

        /**
         * @return The number of nodes.
         */
        public int getNumNodes() {
            return nodes.size();
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "|ExecutionPlan-" + getNumNodes() + " nodes|";
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
                                    @Nonnull Topic tailNodeTopic) throws DataTypeNotMatchedException {
            if (headNode.getStreamInfo().OUTPUT_TYPE != tailNodeTopic.INPUT_TYPE) {
                throw new DataTypeNotMatchedException("Output INPUT_TYPE of stream "
                        + headNode.getStreamInfo() + " does not match with input INPUT_TYPE of topic"
                        + tailNodeTopic);
            }
            if (tailNodeTopic.STREAM_INFO != null && !nodes.containsKey(tailNodeTopic.STREAM_INFO)) {
                addNode(tailNodeTopic.STREAM_INFO);
            }
            headNode.addSuccessor(tailNodeTopic);
        }

        /**
         * Add a node to the execution plan. If it has been added previously with no
         * execution data, the new execution data will be added to the previous node.
         *
         * @param streamInfo Information of the stream of the node.
         * @return The new added node.
         */
        public Node addNode(@Nonnull Stream.Info streamInfo) {
            return addNode(streamInfo, null);
        }

        /**
         * Add a node to the execution plan. If it has been added previously with no
         * execution data, the new execution data will be added to the previous node.
         *
         * @param streamInfo Information of the stream of the node.
         * @param execData   Data for execution of the node.
         * @return The new added node.
         */
        public Node addNode(@Nonnull Stream.Info streamInfo,
                            @Nullable Serializable execData) {
            if (!nodes.containsKey(streamInfo)) {
                Node node = new Node(streamInfo, execData);
                nodes.put(streamInfo, node);
                return node;
            } else {
                Node node = nodes.get(streamInfo);
                if (node.execData == null) {
                    node.execData = execData;
                }
                return node;
            }
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
             * Each node has its own successor nodes, each organized in a list.
             * The indexes of the set correspond to that of the nodes.
             */
            private List<Topic> successorList = new ObjectArrayList<>();

            private final Stream.Info streamInfo;

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
             * @return Name of the stream of the node.
             */
            public Stream.Info getStreamInfo() {
                return streamInfo;
            }

            /**
             * @param execData The data for execution, which is a serializable
             *                 object.
             */
            public Node(Stream.Info streamInfo, Serializable execData) {
                this.streamInfo = streamInfo;
                this.execData = execData;
            }

            /**
             * @return Successor nodes of this node.
             */
            public List<Topic> getSuccessors() {
                return successorList;
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
                this.successorList = null;
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

            /**
             * Add a node to the successor set of this node.
             *
             * @param topic An input of the node to add.
             */
            private void addSuccessor(Topic topic) {
                successorList.add(topic);
            }
        }
    }
}
