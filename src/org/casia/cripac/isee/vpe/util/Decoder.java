package org.cripac.isee.vpe.util;

public class Decoder {

	public static class VideoInfo {
		public int width;
		public int height;
		public int channels;
	}

	private long nativeDecoder = 0;

	private native long initialize(byte[] videoData);

	private native byte[] nextFrame(long nativeDecoder);

	private native void free(long nativeDecoder);

	private native int getWidth(long nativeDecoder);

	private native int getHeight(long nativeDecoder);

	private native int getChannels(long nativeDecoder);

	static {
		System.out.println(System.getProperty("java.library.path"));
		System.loadLibrary("mem-decoding");
	}

	public Decoder(byte[] videoData) {
		nativeDecoder = initialize(videoData);
	}

	public byte[] nextFrame() {
		return nextFrame(nativeDecoder);
	}

	public VideoInfo getVideoInfo() {
		VideoInfo info = new VideoInfo();
		info.channels = getChannels(nativeDecoder);
		info.height = getHeight(nativeDecoder);
		info.width = getWidth(nativeDecoder);
		return info;
	}

	@Override
	protected void finalize() throws Throwable {
		free(nativeDecoder);
		super.finalize();
	}
}
