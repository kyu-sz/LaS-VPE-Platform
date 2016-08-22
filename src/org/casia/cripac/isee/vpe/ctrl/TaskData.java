/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.vpe.ctrl;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * The TaskData class contains a global execution plan and the execution result
 * of the predecessor node.
 * 
 * @author Ken Yu, CRIPAC, 2016
 */
public class TaskData implements Serializable, Cloneable {

	private static final long serialVersionUID = -7488783279604501871L;

	/**
	 * The ExecutionPlan class represents a directed acyclic graph of the
	 * execution flows of modules. Each node is an execution of a module. Each
	 * link represents that an execution should output to a next execution node.
	 * One module may exist multiple times in a graph.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 */
	public static class ExecutionPlan implements Serializable, Cloneable {

		/**
		 * Each node represents an execution of a module.
		 * 
		 * @author Ken Yu, CRIPAC, 2016
		 *
		 */
		public static class Node implements Serializable, Cloneable {

			private static final long serialVersionUID = 1111630850962155197L;

			/**
			 * The input topic of the module to be executed in the node.
			 */
			public String topic;

			/**
			 * The data for this execution.
			 */
			public Serializable executionData = null;

			/**
			 * Counter for unexecuted predecessors.
			 */
			public int unexecutedPredecessorCnt = 0;

			/**
			 * Create a node specifying only the input topic of the module to be
			 * executed in the node.
			 * 
			 * @param topic
			 *            The input topic of the module to be executed in the
			 *            node.
			 */
			public Node(String topic) {
				this.topic = topic;
			}

			/**
			 * Create a node specifying the input topic of the module to be
			 * executed in the node and the data for execution.
			 * 
			 * @param topic
			 *            The input topic of the module to be executed in the
			 *            node.
			 * @param executionData
			 *            The data for execution, which is a serializable
			 *            object.
			 */
			public Node(String topic, Serializable executionData) {
				this.topic = topic;
				this.executionData = executionData;
			}
		}

		private static final long serialVersionUID = -4268794570615111388L;

		/**
		 * Each node represents a module to execute.
		 */
		private Node[] nodes;

		private int numNodes = 0;

		/**
		 * Each node has its own successor nodes, each organized in a set. The
		 * indexes of the sets correspond to that of the nodes.
		 */
		private Set<Integer>[] successors;

		/**
		 * Markers recording whether each node has been executed.
		 */
		private boolean[] executed;

		/**
		 * Create a link from the head node to the tail node.
		 * 
		 * @param head
		 *            The ID of the head node.
		 * @param tail
		 *            The ID of the tail node.
		 */
		public void linkNodes(int head, int tail) {
			successors[head].add(tail);
			if (!executed[head]) {
				nodes[tail].unexecutedPredecessorCnt++;
			}
		}

		/**
		 * Add a node specifying only the input topic of the module to be
		 * executed in the node.
		 * 
		 * @param topic
		 *            The input topic of the module to be executed in the node.
		 * @return The ID of the node.
		 */
		public int addNode(String topic) {
			nodes[numNodes] = new Node(topic);
			return numNodes++;
		}

		/**
		 * Add a node specifying the input topic of the module to be executed in
		 * the node and the data for execution.
		 * 
		 * @param topic
		 *            The input topic of the module to be executed in the node.
		 * @param executionData
		 *            The data for execution, which is a serializable object.
		 * @return The ID of the node.
		 */
		public int addNode(String topic, Serializable executionData) {
			nodes[numNodes] = new Node(topic, executionData);
			return numNodes++;
		}

		/**
		 * Append a node to a head node specifying only the input topic of the
		 * module to be executed in the node.
		 * 
		 * @param head
		 *            The ID of the head node.
		 * @param topic
		 *            The input topic of the module to be executed in the node.
		 * @return The ID of the node.
		 */
		public int appendNode(int head, String topic) {
			nodes[numNodes] = new Node(topic);
			linkNodes(head, numNodes);
			return numNodes++;
		}

		/**
		 * Append a node to a head node specifying the input topic of the module
		 * to be executed in the node and the data for execution.
		 * 
		 * @param head
		 *            The ID of the head node.
		 * @param topic
		 *            The input topic of the module to be executed in the node.
		 * @param executionData
		 *            The data for execution, which is a serializable object.
		 * @return The ID of the node.
		 */
		public int appendNode(int head, String topic, Serializable executionData) {
			nodes[numNodes] = new Node(topic, executionData);
			linkNodes(head, numNodes);
			return numNodes++;
		}

		/**
		 * Get the IDs of all startable nodes. A node is startable when all its
		 * predecessors have been executed.
		 * 
		 * @return IDs of all startable nodes.
		 */
		public Set<Integer> getStartableNodes() {
			Set<Integer> startableNodes = new HashSet<>();
			for (int i = 0; i < nodes.length; ++i) {
				if (!executed[i] && nodes[i].unexecutedPredecessorCnt == 0) {
					startableNodes.add(i);
				}
			}
			return startableNodes;
		}

		/**
		 * Initialize an execution plan specifying the total number of nodes.
		 * 
		 * @param maxNumNodes
		 *            Maximum number of nodes.
		 */
		@SuppressWarnings("unchecked")
		public ExecutionPlan(int maxNumNodes) {
			nodes = new Node[maxNumNodes];
			successors = new Set[maxNumNodes];
			executed = new boolean[maxNumNodes];
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#clone()
		 */
		@Override
		protected Object clone() {
			ExecutionPlan clonedPlan = null;
			try {
				clonedPlan = (ExecutionPlan) super.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
				return null;
			}
			clonedPlan.successors = this.successors.clone();
			clonedPlan.nodes = this.nodes.clone();
			clonedPlan.executed = this.executed.clone();
			return clonedPlan;
		}

		/**
		 * Recommended to call before the getPlanForNextNode method to save
		 * computation resource.
		 * 
		 * @param nodeID
		 *            The ID of the node to be marked executed.
		 */
		public void markExecuted(int nodeID) {
			if (!executed[nodeID]) {
				successors[nodeID] = null;
				nodes[nodeID] = null;
				executed[nodeID] = true;

				for (int successorID : successors[nodeID]) {
					--nodes[successorID].unexecutedPredecessorCnt;
				}
			}
		}

		/**
		 * Get the execution plan for the execution of a successor node of the
		 * current node. The id should be retrieved from the successor set of
		 * the current node.
		 * 
		 * @param currentNodeID
		 *            The ID of the current node.
		 * @param nextNodeID
		 *            The ID of the next node to execute. Should be retrieved
		 *            from the successor set of the current node.
		 * @return The plan for that execution.
		 */
		public ExecutionPlan getPlanForNextNode(int currentNodeID, int nodeID, Object result) {
			// Clone a plan.
			ExecutionPlan plan = (ExecutionPlan) this.clone();

			// Remove current node.
			plan.markExecuted(currentNodeID);

			return plan;
		}

		/**
		 * To invoke a node, we only need to know one of the input topics of the
		 * module corresponding to it.
		 * 
		 * @param nodeID
		 *            The ID of the node.
		 * @return The name of a topic specified in the execution plan for the
		 *         current module to output to.
		 */
		public String getInputTopicName(int nodeID) {
			return nodes[nodeID].topic;
		}

		/**
		 * Get the successor nodes of a node.
		 * 
		 * @param nodeID
		 *            The ID of the node.
		 * @return A set of the successor nodes.
		 */
		public Set<Integer> getSuccessors(int nodeID) {
			return successors[nodeID];
		}

		/**
		 * Get the execution data of a node.
		 * 
		 * @param nodeID
		 *            The ID of the node.
		 * @return Execution data in byte array.
		 */
		public Serializable getExecutionData(int nodeID) {
			return nodes[nodeID].executionData;
		}

		/**
		 * Combine another execution plan to form a new plan. This function does
		 * not affect the original plan.
		 * 
		 * @param plan
		 *            A plan to combine to the current plan.
		 * @return A new plan combining the current plan and the given plan.
		 */
		public ExecutionPlan combine(ExecutionPlan plan) {
			ExecutionPlan combinedPlan = (ExecutionPlan) this.clone();
			if (plan != null) {
				for (int i = 0; i < combinedPlan.nodes.length; ++i) {
					if (combinedPlan.executed[i] || plan.executed[i]) {
						combinedPlan.executed[i] = true;
						combinedPlan.nodes[i] = null;
						combinedPlan.successors[i] = null;
					} else {
						if (combinedPlan.nodes[i] == null) {
							combinedPlan.nodes[i] = plan.nodes[i];
						} else {
							if (plan.nodes[i] != null) {
								assert (combinedPlan.nodes[i].equals(plan.nodes[i]));
							}
						}
						if (combinedPlan.successors[i] == null) {
							combinedPlan.successors[i] = plan.successors[i];
						} else {
							if (plan.successors[i] != null) {
								assert (combinedPlan.successors[i].equals(plan.successors[i]));
							}
						}
					}
				}
			}
			return combinedPlan;
		}
	}

	/**
	 * The ID of the current node.
	 */
	public int currentNodeID;

	/**
	 * The global execution plan.
	 */
	public ExecutionPlan executionPlan = null;

	/**
	 * The result of the predecessor.
	 */
	public Serializable predecessorResult = null;

	/**
	 * Create a task with an execution plan with no predecessor result.
	 * 
	 * @param currentNodeID
	 *            The ID of the current node to execute.
	 * @param executionPlan
	 *            A global execution plan.
	 */
	public TaskData(int currentNodeID, ExecutionPlan executionPlan) {
		this.currentNodeID = currentNodeID;
		this.executionPlan = executionPlan;
	}

	/**
	 * Create a task with an execution plan with predecessor result.
	 * 
	 * @param currentNodeID
	 *            The ID of the current node to execute.
	 * @param executionPlan
	 *            A global execution plan.
	 * @param predecessorResult
	 *            Result of the predecessor node, which is a serializable
	 *            object.
	 */
	public TaskData(int currentNodeID, ExecutionPlan executionPlan, Serializable predecessorResult) {
		this.currentNodeID = currentNodeID;
		this.executionPlan = executionPlan;
		this.predecessorResult = predecessorResult;
	}
}
