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

package org.cripac.isee.vpe.util.tracking;

import javax.annotation.Nonnull;

/**
 * The class VideoDecoder utilizes native libraries including FFMPEG to decode
 * videos stored in the memory.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class VideoDecoder {

    static {
        System.out.println(
                "Loading native libraries for VideoDecoder from "
                        + System.getProperty("java.library.path"));
        System.loadLibrary("video_decoder_jni");
    }

    private long nativeDecoder = 0;

    /**
     * Create a decoder for one specific video, whose raw byte data is stored in
     * memory and passed as videoData.
     *
     * @param videoData The raw byte data of the video.
     * @throws NullPointerException On videoData is null.
     */
    public VideoDecoder(@Nonnull byte[] videoData) {
        if (videoData == null) {
            throw new NullPointerException("Byte array of video data cannot be null!");
        }
        nativeDecoder = initialize(videoData);
    }

    private native long initialize(@Nonnull byte[] videoData);

    private native byte[] nextFrame(long nativeDecoder);

    private native int skipFrame(long nativeDecoder, int numFrames);

    private native void free(long nativeDecoder);

    private native int getWidth(long nativeDecoder);

    private native int getHeight(long nativeDecoder);

    private native int getChannels(long nativeDecoder);

    /**
     * Get a next frame decoded.
     *
     * @return Byte data of the next frame or null if there is no next frame.
     */
    public byte[] nextFrame() {
        return nextFrame(nativeDecoder);
    }

    /**
     * Get information of the video being decoded.
     *
     * @return A structure containing the information of the video.
     */
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

    /**
     * Information of a video.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class VideoInfo {
        public int width;
        public int height;
        public int channels;
    }
}
