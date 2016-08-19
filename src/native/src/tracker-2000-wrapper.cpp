#include "../include/org_casia_cripac_isee_pedestrian_tracking_NativeTracker2000.h"
#include "../include/basic_define.h"
#include "../include/tracking_intf.h"

//TODO Implement this function
char* getBytesFromJNI(jbyteArray array);

JNIEXPORT jobjectArray JNICALL Java_org_casia_cripac_isee_pedestrian_tracking_NativeTracker2000_nativeTrack
  (JNIEnv *env, jobject obj, jint width, jint height, jint channels, jbyteArray jconfig, jobjectArray jframes)
{
	char* config = getBytesFromJNI(jconfig);
	ObjTracking tracker = new ObjTracking();

	//TODO Check how to get length from jobjectArray

	tracker.init(width, height, channels, config, jconfig.length);

	int numFrames = jframes.length;
	char** frames = new char*[numFrames];

	//TODO Transfer frames from jframes.

	for (int i = 0; i < numFrames; ++i)
	{
		tracker.doTrack(frames[i]);
		delete[] frames[i];
	}

	delete[] frames;
	delete[] config;

	TrackList trackList;
	int numTracks = tracker.getTargets(trackList);

	jobjectArray jtracks;
	
	//TODO Transfer C++ tracks to JNI.

	return jtracks;
}