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
import java.util.ArrayList;
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
		public class Node implements Serializable, Cloneable {

			private static final long serialVersionUID = 1111630850962155197L;

			/**
			 * The input topic of the module to be executed in the node.
			 */
			public String topic = null;

			/**
			 * The data for this execution.
			 */
			public Serializable executionData = null;

			/**
			 * Counter for unexecuted predecessors.
			 */
			public int unexecutedPredecessorCnt = 0;

			/**
			 * Each node has its own successor nodes, each organized in a set. The
			 * indexes of the set correspond to that of the nodes.
			 */
			public Set<Integer> successors = null;

			/**
			 * Marker recording whether the node has been executed.
			 */
			boolean executed = false;

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
				this.successors = new HashSet<>();
			}
			
			/**
			 * Create an empty node for saving memory.
			 */
			public Node() {}
			
			/**
			 * Check whether the node is empty.
			 * @return	A boolean marker indicating whether the node is empty.
			 */
			public boolean isEmpty() {
				return (this.successors == null && this.topic == null && this.executionData == null);
			}
			
			/**
			 * Empty the data in the node for saving memory.
			 */
			public void makeEmpty() {
				this.successors = null;
				this.topic = null;
				this.executionData = null;
			}
			
			/**
			 * Mark the node as executed and clear its data.
			 * Update its successors' unexecutedPredecessorCnt field.
			 */
			public void markExecuted() {
				if (!executed) {
					executed = true;
					for (int successorID : successors) {
						--nodes.get(successorID).unexecutedPredecessorCnt;
					}
					makeEmpty();
				}
			}
		}

		private static final long serialVersionUID = -4268794570615111388L;

		/**
		 * Each node represents a module to execute.
		 */
		private ArrayList<Node> nodes = new ArrayList<>();

		/**
		 * Create a link from the head node to the tail node.
		 * 
		 * @param head
		 *            The ID of the head node.
		 * @param tail
		 *            The ID of the tail node.
		 */
		public void linkNodes(int head, int tail) {
			Node headNode = nodes.get(head);
			headNode.successors.add(tail);
			if (!headNode.executed) {
				nodes.get(tail).unexecutedPredecessorCnt++;
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
			nodes.add(new Node(topic));
			return nodes.size() - 1;
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
			nodes.add(new Node(topic, executionData));
			return nodes.size() - 1;
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
			nodes.add(new Node(topic));
			int id = nodes.size() - 1;
			linkNodes(head, id);
			return id;
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
			nodes.add(new Node(topic, executionData));
			int id = nodes.size() - 1;
			linkNodes(head, id);
			return id;
		}

		/**
		 * Get the IDs of all startable nodes. A node is startable when all its
		 * predecessors have been executed.
		 * 
		 * @return IDs of all startable nodes.
		 */
		public Set<Integer> getStartableNodes() {
			Set<Integer> startableNodes = new HashSet<>();
			for (int i = 0; i < nodes.size(); ++i) {
				Node node = nodes.get(i);
				if (!node.executed && node.unexecutedPredecessorCnt == 0) {
					startableNodes.add(i);
				}
			}
			return startableNodes;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#clone()
		 */
		@SuppressWarnings("unchecked")
		@Override
		protected Object clone() {
			ExecutionPlan clonedPlan = null;
			try {
				clonedPlan = (ExecutionPlan) super.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
				return null;
			}
			clonedPlan.nodes = (ArrayList<Node>) this.nodes.clone();
			return clonedPlan;
		}

		/**
		 * Mark a node as executed.
		 * Recommended to call before the getPlanForNextNode method to save
		 * computation resource.
		 * 
		 * @param nodeID
		 *            The ID of the node to be marked executed.
		 */
		public void markExecuted(int nodeID) {
			nodes.get(nodeID).markExecuted();;
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
			return nodes.get(nodeID).topic;
		}

		/**
		 * Get the successor nodes of a node.
		 * 
		 * @param nodeID
		 *            The ID of the node.
		 * @return A set of the successor nodes.
		 */
		public Set<Integer> getSuccessors(int nodeID) {
			return nodes.get(nodeID).successors;
		}

		/**
		 * Get the execution data of a node.
		 * 
		 * @param nodeID
		 *            The ID of the node.
		 * @return Execution data in byte array.
		 */
		public Serializable getExecutionData(int nodeID) {
			return nodes.get(nodeID).executionData;
		}

		/**
		 * Combine another execution plan to form a new plan. This function does
		 * not affect the original plan.
		 * 
		 * @param externPlan
		 *            A plan to combine to the current plan.
		 * @return A new plan combining the current plan and the given plan.
		 */
		public ExecutionPlan combine(ExecutionPlan externPlan) {
			ExecutionPlan combinedPlan = (ExecutionPlan) this.clone();
			if (externPlan != null) {
				assert(externPlan.nodes.size() == combinedPlan.nodes.size());
				for (int i = 0; i < combinedPlan.nodes.size(); ++i) {
					Node combinedNode = combinedPlan.nodes.get(i);
					Node externNode = externPlan.nodes.get(i);
					if (combinedNode.executed || externNode.executed) {
						combinedNode.markExecuted();
					} else {
						if (combinedNode.isEmpty()) {
							combinedPlan.nodes.set(i, externNode);
						} else {
							if (!externNode.isEmpty()) {
								assert (combinedNode.equals(externNode));
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
