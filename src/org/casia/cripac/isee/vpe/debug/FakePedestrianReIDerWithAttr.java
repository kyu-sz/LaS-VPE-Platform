/***********************************************************************
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
 * along with LaS-VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.vpe.debug;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Random;

import org.casia.cripac.isee.pedestrian.reid.PedestrianComparerWithAttr;
import org.casia.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.casia.cripac.isee.pedestrian.reid.PedestrianReIDer;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class FakePedestrianReIDerWithAttr extends PedestrianReIDer {

	private Random rand = new Random();
	private PedestrianComparerWithAttr comparer = new PedestrianComparerWithAttr() {

		@Override
		public float compare(PedestrianInfo personA, PedestrianInfo personB) {
			return rand.nextFloat();
		}
	};
	private PedestrianInfo[] dbCache;

	public FakePedestrianReIDerWithAttr() {
		FakeDatabaseConnector dbConnector = new FakeDatabaseConnector();
		dbCache = new PedestrianInfo[100];
		for (int i = 0; i < dbCache.length; ++i) {
			dbCache[i] = dbConnector.getTrackWithAttr("sample", "0");
			dbCache[i].id = i;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.pedestrian.reid.PedestrianReIDerWithAttr#reid(org.
	 * casia.cripac.isee.pedestrian.tracking.Track,
	 * org.casia.cripac.isee.pedestrian.attr.Attribute)
	 */
	@Override
	public int[] reid(PedestrianInfo target) throws IOException {
		class SimilarityWithID implements Comparable<SimilarityWithID> {
			int id;
			float similarity;

			public SimilarityWithID(int id, float similarity) {
				this.id = id;
				this.similarity = similarity;
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see java.lang.Comparable#compareTo(java.lang.Object)
			 */
			@Override
			public int compareTo(SimilarityWithID o) {
				if (similarity > o.similarity) {
					return 1;
				} else if (similarity == o.similarity) {
					return 0;
				} else {
					return -1;
				}
			}
		}

		PriorityQueue<SimilarityWithID> sorter = new PriorityQueue<>();
		for (PedestrianInfo stored : dbCache) {
			sorter.add(new SimilarityWithID(target.id, comparer.compare(target, stored)));
		}

		int[] rank = new int[10];
		for (int i = 0; i < 10; ++i) {
			rank[i] = sorter.poll().id;
		}
		return rank;
	}

}
