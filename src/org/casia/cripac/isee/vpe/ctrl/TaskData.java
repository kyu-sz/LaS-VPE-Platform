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
import java.util.Set;

/**
 * The TaskData class contains a global execution plan and the execution result of the predecessor node.
 * @author Ken Yu, CRIPAC, 2016
 */
public class TaskData implements Serializable, Cloneable {
	
	private static final long serialVersionUID = -7488783279604501871L;
	
	/**
	 * The ExecutionPlan class represents a directed acyclic graph of the execution flows of modules.
	 * Each node is an execution of a module.
	 * Each link represents that an execution should output to a next execution node.
	 * One module may exist multiple times in a graph.
	 * @author Ken Yu, CRIPAC, 2016
	 */
	public static class ExecutionPlan implements Serializable, Cloneable {

		/**
		 * Each node represents an execution of a module.
		 * @author Ken Yu, CRIPAC, 2016
		 *
		 */
		public static class Node implements Serializable, Cloneable {

			private static final long serialVersionUID = 1111630850962155197L;

			/**
			 * The invoking topic of the module corresponding to this node.
			 */
			public String topic;
			
			/**
			 * The data for this execution.
			 */
			public byte[] executionData = null;
		}

		private static final long serialVersionUID = -4268794570615111388L;

		/**
		 * Each node represents a module to execute.
		 */
		private Node[] nodes;
		
		/**
		 * Each node has its own successor nodes, each organized in a set.
		 * The indexes of the sets correspond to that of the nodes.
		 */
		private Set<Integer>[] successors;
		
		/**
		 * The ID of the current node.
		 */
		private int currentNode;
		
		/**
		 * Markers recording whether each node has been executed.
		 */
		private boolean[] executed;

		/**
		 * Initialize an execution plan specifying the total number of nodes. 
		 * @param numNodes	Number of nodes.
		 */
		@SuppressWarnings("unchecked")
		public ExecutionPlan(int numNodes) {
			nodes = new Node[numNodes];
			successors = new Set[numNodes];
			executed = new boolean[numNodes];
			currentNode = 0;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#clone()
		 */
		@Override
		protected Object clone() {
			ExecutionPlan clonedPlan = null;
			try {
				clonedPlan = (ExecutionPlan)super.clone();
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
		 * Recommended to call before the getPlanForNextNode method to save computation resource.
		 */
		public void markExecuted() {
			successors[currentNode] = null;
			nodes[currentNode] = null;
			executed[currentNode] = true;
		}
		
		/**
		 * Get the execution plan for the execution of a successor node of the current node.
		 * The id should be retrieved from the successor set of the current node.
		 * @param nodeID	The ID of the next node to execute.
		 * @return			The plan for that execution.
		 */
		public ExecutionPlan getPlanForNextNode(int nodeID, Object result) {
			// Clone a plan.
			ExecutionPlan plan = (ExecutionPlan) this.clone();
			
			// Remove 
			if (plan.successors[currentNode] != null)
				plan.successors[currentNode] = null;
			if (plan.nodes[currentNode] != null)
				plan.nodes[currentNode] = null;
			plan.executed[currentNode] = true;
			
			plan.currentNode = nodeID;
			
			return plan;
		}
		
		/**
		 * To invoke a node, we only need to know one of the input topics of the module corresponding to it.
		 * @param nodeID	The ID of the node.
		 * @return			The name of a topic specified in the execution plan for the current module to output to.
		 */
		public String getInputTopicName(int nodeID) {
			return nodes[nodeID].topic;
		}
		
		/**
		 * Get the successor nodes of the current node.
		 * @return	A set of the successor nodes.
		 */
		public Set<Integer> getSuccessors() {
			return successors[currentNode];
		}
		
		/**
		 * Get the execution data for the current node.
		 * @return	Execution data in byte array.
		 */
		public byte[] getExecutionData() {
			return nodes[currentNode].executionData;
		}
		
		/**
		 * Combine another execution plan.
		 * @param plan	A plan to combine to the current plan.
		 */
		public void combine(ExecutionPlan plan) {
			for (int i = 0; i < nodes.length; ++i) {
				if (executed[i] || plan.executed[i]) {
					executed[i] = true;
					nodes[i] = null;
					successors[i] = null;
				} else {
					if (nodes[i] == null) {
						nodes[i] = plan.nodes[i];
					} else {
						if (plan.nodes[i] != null) {
							assert(nodes[i].equals(plan.nodes[i]));
						}
					}
					if (successors[i] == null) {
						successors[i] = plan.successors[i];
					} else {
						if (plan.successors[i] != null) {
							assert(successors[i].equals(plan.successors[i]));
						}
					}
				}
			}
		}
	}

	/**
	 * The global execution plan.
	 */
	public ExecutionPlan executionPlan;
	
	/**
	 * The result of the predecessor.
	 */
	public Object predecessorResult; 
	
	/**
	 * Initialize while specifying the total number of nodes for initializing the execution plan. 
	 * @param numNodes	Number of nodes.
	 */
	public TaskData(int numNodes) {
		executionPlan = new ExecutionPlan(0);
		predecessorResult = null;
	}
}
