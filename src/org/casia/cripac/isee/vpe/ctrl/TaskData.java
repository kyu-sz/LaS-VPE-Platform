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
import java.util.Iterator;

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
	public static abstract class ExecutionPlan implements Serializable, Cloneable {

		/**
		 * Each node represents an execution of a module.
		 * 
		 * @author Ken Yu, CRIPAC, 2016
		 *
		 */
		public abstract class Node implements Serializable, Cloneable {

			private static final long serialVersionUID = 4538251384004287468L;

			/**
			 * The input topic of the module to be executed in the node.
			 */
			private char[] topic = null;

			/**
			 * The data for this execution.
			 */
			private Serializable executionData = null;

			/**
			 * Counter for unexecuted predecessors.
			 */
			private int unexecutedPredecessorCnt = 0;

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
				this.topic = topic.toCharArray();
			}

			/**
			 * Create an empty node for saving memory.
			 */
			public Node() {
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
				this.topic = topic.toCharArray();
				this.executionData = executionData;
			}

			/**
			 * @return the topic
			 */
			public String getTopic() {
				return new String(topic);
			}

			/**
			 * @return the executionData
			 */
			public Serializable getExecutionData() {
				return executionData;
			}

			/**
			 * @return The number of unexecuted predecessor nodes.
			 */
			public int getNumUnexecutedPredecessor() {
				return unexecutedPredecessorCnt;
			}

			/**
			 * @return Whether the node has been executed.
			 */
			public boolean isExecuted() {
				return executed;
			}

			/**
			 * Check whether the node is empty.
			 * 
			 * @return Whether the node is empty.
			 */
			public boolean isEmpty() {
				return (this.topic == null && this.executionData == null);
			}

			/**
			 * Empty the data in the node for saving memory.
			 */
			public void makeEmpty() {
				this.topic = null;
				this.executionData = null;
			}

			/**
			 * Mark the node as executed and clear its data. Update its
			 * successors' unexecutedPredecessorCnt field.
			 */
			public void markExecuted() {
				if (!executed) {
					executed = true;
					for (int successorID : getSuccessors()) {
						getNode(successorID).decreaseUnexecutedPredecessorCnt();
					}
					makeEmpty();
				}
			}

			/**
			 * @return IDs of successor nodes of this node.
			 */
			public abstract int[] getSuccessors();

			public void setUnexecutedPredecessorCnt(int cnt) {
				unexecutedPredecessorCnt = cnt;
			}

			public void increaseUnexecutedPredecessorCnt() {
				++unexecutedPredecessorCnt;
			}

			public void decreaseUnexecutedPredecessorCnt() {
				--unexecutedPredecessorCnt;
			}
		}

		/**
		 * Each node represents an execution of a module.
		 * 
		 * @author Ken Yu, CRIPAC, 2016
		 *
		 */
		public class MutableNode extends Node {

			private static final long serialVersionUID = 1111630850962155197L;

			/**
			 * Each node has its own successor nodes, each organized in a list.
			 * The indexes of the set correspond to that of the nodes.
			 */
			private ArrayList<Integer> successorList = null;

			/**
			 * Create a node specifying only the input topic of the module to be
			 * executed in the node.
			 * 
			 * @param topic
			 *            The input topic of the module to be executed in the
			 *            node.
			 */
			public MutableNode(String topic) {
				super(topic);
				this.successorList = new ArrayList<>();
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
			public MutableNode(String topic, Serializable executionData) {
				super(topic, executionData);
				this.successorList = new ArrayList<>();
			}

			/**
			 * Create an empty node for saving memory.
			 */
			public MutableNode() {
			}

			@Override
			public boolean isEmpty() {
				return (this.successorList == null && super.isEmpty());
			}

			@Override
			public void makeEmpty() {
				this.successorList = null;
				super.makeEmpty();
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan.Node#
			 * getSuccessors()
			 */
			@Override
			public int[] getSuccessors() {
				int[] res = new int[successorList.size()];
				Iterator<Integer> successorListIter = successorList.iterator();
				for (int i = 0; i < successorList.size(); ++i) {
					res[i] = successorListIter.next();
				}
				return res;
			}

			/**
			 * @return An immutable copy of this node.
			 */
			public ImmutableNode createImmutableCopy() {
				ImmutableNode immutableCopy = new ImmutableNode(getTopic(), getExecutionData(), getSuccessors());
				immutableCopy.setUnexecutedPredecessorCnt(getNumUnexecutedPredecessor());
				return immutableCopy;
			}

			/**
			 * Add a node to the successor set of this node.
			 * 
			 * @param nodeID
			 *            The ID of the node to add.
			 */
			public void addSuccessor(int nodeID) {
				successorList.add(nodeID);
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see java.lang.Object#clone()
			 */
			@SuppressWarnings("unchecked")
			@Override
			protected Object clone() throws CloneNotSupportedException {
				MutableNode clonedPlan = null;
				try {
					clonedPlan = (MutableNode) super.clone();
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
					return null;
				}
				this.successorList = (ArrayList<Integer>) this.successorList.clone();
				return clonedPlan;
			}
		}

		public class ImmutableNode extends Node {

			private static final long serialVersionUID = 6094926375767352113L;

			private int[] successors = null;

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan.Node#
			 * getSuccessors()
			 */
			@Override
			public int[] getSuccessors() {
				return successors;
			}

			/**
			 * Create a node specifying only the input topic of the module to be
			 * executed in the node.
			 * 
			 * @param topic
			 *            The input topic of the module to be executed in the
			 *            node.
			 * @param successors
			 *            Successor nodes of this node.
			 */
			public ImmutableNode(String topic, int[] successors) {
				super(topic);
				this.successors = successors;
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
			 * @param successors
			 *            Successor nodes of this node.
			 */
			public ImmutableNode(String topic, Serializable executionData, int[] successors) {
				super(topic, executionData);
				this.successors = successors;
			}

			/**
			 * Create an empty node for saving memory.
			 */
			public ImmutableNode() {
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see java.lang.Object#clone()
			 */
			@Override
			protected Object clone() throws CloneNotSupportedException {
				MutableNode clonedPlan = null;
				try {
					clonedPlan = (MutableNode) super.clone();
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
					return null;
				}
				this.successors = this.successors.clone();
				return clonedPlan;
			}
		}

		private static final long serialVersionUID = 1359845978045759483L;

		/**
		 * Get the IDs of all startable nodes. A node is startable when all its
		 * predecessors have been executed.
		 * 
		 * @return IDs of all startable nodes.
		 */
		public Integer[] getStartableNodes() {
			ArrayList<Integer> startableNodes = new ArrayList<>();
			for (int i = 0; i < getNumNodes(); ++i) {
				Node node = getNode(i);
				if (!node.executed && node.unexecutedPredecessorCnt == 0) {
					startableNodes.add(i);
				}
			}
			Integer[] nodes = new Integer[startableNodes.size()];
			return startableNodes.toArray(nodes);
		}

		/**
		 * Mark a node as executed. Recommended to call before the
		 * getPlanForNextNode method to save computation resource.
		 * 
		 * @param nodeID
		 *            The ID of the node to be marked executed.
		 */
		public void markExecuted(int nodeID) {
			getNode(nodeID).markExecuted();
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#clone()
		 */
		@Override
		protected Object clone() {
			try {
				return super.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
				return null;
			}
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
				assert (externPlan.getNumNodes() == combinedPlan.getNumNodes());
				for (int i = 0; i < combinedPlan.getNumNodes(); ++i) {
					Node combinedNode = combinedPlan.getNode(i);
					Node externNode = externPlan.getNode(i);
					if (combinedNode.executed || externNode.executed) {
						combinedNode.markExecuted();
					} else {
						if (combinedNode.isEmpty()) {
							combinedPlan.updateNode(i, externNode);
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

		/**
		 * Get a node by ID.
		 * 
		 * @param nodeID
		 *            The ID of the node.
		 * @return Node with this ID.
		 */
		public abstract Node getNode(int nodeID);

		/**
		 * @return The number of nodes.
		 */
		public abstract int getNumNodes();

		/**
		 * Update a node.
		 * 
		 * @param nodeID
		 *            The ID of the node to be updated.
		 * @param node
		 *            The new node.
		 */
		public abstract void updateNode(int nodeID, Node node);

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "|ExecutionPlan-" + getNumNodes() + " nodes|";
		}
	}

	public static class ImmutableExecutionPlan extends ExecutionPlan {

		private static final long serialVersionUID = 2678777741259466998L;
		/**
		 * Each node represents a module to execute.
		 */
		private Node[] nodes = null;

		/**
		 * Create an immutable execution plan with nodes specified.
		 * 
		 * @param nodes
		 */
		public ImmutableExecutionPlan(ImmutableNode[] nodes) {
			this.nodes = nodes;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan#getNode(int)
		 */
		@Override
		public Node getNode(int nodeID) {
			return nodes[nodeID];
		}

		@Override
		public int getNumNodes() {
			return nodes.length;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#clone()
		 */
		@Override
		protected Object clone() {
			ImmutableExecutionPlan clonedPlan = (ImmutableExecutionPlan) super.clone();
			clonedPlan.nodes = this.nodes.clone();
			return clonedPlan;
		}

		public ImmutableNode adapt(Node node) {
			if (node instanceof ImmutableNode) {
				return (ImmutableNode) node;
			} else {
				return ((MutableNode) node).createImmutableCopy();
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan#updateNode(int,
		 * org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan.Node)
		 */
		@Override
		public void updateNode(int nodeID, Node node) {
			nodes[nodeID] = node;
		}
	}

	public static class MutableExecutionPlan extends ExecutionPlan {

		private static final long serialVersionUID = -4268794570615111388L;

		/**
		 * Each node represents a module to execute.
		 */
		private ArrayList<MutableNode> nodes = new ArrayList<>();

		/**
		 * Create a link from the head node to the tail node.
		 * 
		 * @param head
		 *            The ID of the head node.
		 * @param tail
		 *            The ID of the tail node.
		 */
		public void linkNodes(int head, int tail) {
			MutableNode headNode = nodes.get(head);
			headNode.addSuccessor(tail);
			MutableNode tailNode = nodes.get(tail);
			if (!tailNode.executed) {
				tailNode.increaseUnexecutedPredecessorCnt();
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
			nodes.add(new MutableNode(topic));
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
			nodes.add(new MutableNode(topic, executionData));
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
			nodes.add(new MutableNode(topic));
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
			nodes.add(new MutableNode(topic, executionData));
			int id = nodes.size() - 1;
			linkNodes(head, id);
			return id;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#clone()
		 */
		@SuppressWarnings("unchecked")
		@Override
		protected Object clone() {
			MutableExecutionPlan clonedPlan = (MutableExecutionPlan) super.clone();
			clonedPlan.nodes = (ArrayList<MutableNode>) this.nodes.clone();
			return clonedPlan;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan#getNode(int)
		 */
		@Override
		public Node getNode(int nodeID) {
			return nodes.get(nodeID);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan#getNumNodes()
		 */
		@Override
		public int getNumNodes() {
			return nodes.size();
		}

		/**
		 * @return An immutable copy of this plan.
		 */
		public ImmutableExecutionPlan createImmutableCopy() {
			ImmutableNode[] immutableNodes = new ImmutableNode[nodes.size()];
			Iterator<MutableNode> nodeIter = nodes.iterator();
			for (int i = 0; i < nodes.size(); ++i) {
				immutableNodes[i] = nodeIter.next().createImmutableCopy();
			}
			return new ImmutableExecutionPlan(immutableNodes);
		}

		public MutableNode adapt(Node node) {
			if (node instanceof MutableNode) {
				return (MutableNode) node;
			} else {
				MutableNode mutableNode = new MutableNode(node.getTopic(), node.getExecutionData());
				for (int successor : node.getSuccessors()) {
					mutableNode.successorList.add(successor);
				}
				return mutableNode;
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan#updateNode(org.
		 * casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan.Node, int)
		 */
		@Override
		public void updateNode(int nodeID, Node node) {
			nodes.set(nodeID, adapt(node));
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "|TaskData-node=" + currentNodeID + "-ExecutionPlan=" + executionPlan + "-PredecessorRes=\n"
				+ predecessorResult + "|";
	}
}
