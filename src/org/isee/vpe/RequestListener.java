package org.isee.vpe;

import java.net.ServerSocket;

class Request {
	enum Type {
		tracking,
		attr_recog,
	};
	
	Type type;
	
	Request(Type type) {
		this.type = type;
	}
}

class RequestListener {
	ServerSocket serverSocket = null;
	
	RequestListener() {
		
	}
	
	Request await() {
		return new Request(Request.Type.tracking);
	}
}
