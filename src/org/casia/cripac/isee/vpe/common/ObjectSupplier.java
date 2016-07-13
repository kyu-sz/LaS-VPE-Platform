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
package org.casia.cripac.isee.vpe.common;

import java.io.Closeable;
import java.io.Serializable;
import java.util.function.Supplier;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class ObjectSupplier<T> implements Serializable, Supplier<T> {
	
	private static final long serialVersionUID = 4084371413129601845L;
	private volatile T instance = null;
	private ObjectFactory<T> factory = null;
	
	public ObjectSupplier(ObjectFactory<T> factory) {
		this.factory = factory;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		if (instance != null && instance instanceof Closeable) {
			((Closeable) instance).close();
		}
		super.finalize();
	}

	/* (non-Javadoc)
	 * @see java.util.function.Supplier#get()
	 */
	@Override
	public T get() {
		if (instance == null) {
			instance = factory.getObject();
		}
		return instance;
	}

}
