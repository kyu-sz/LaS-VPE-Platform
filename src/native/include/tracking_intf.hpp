/** \file    tracking_inf.h
 *  \brief   The interface for object tracking.
 *  \version 0.1
 *  \date    2016-07-26
 *  \email   da.li@cripac.ia.ac.cn
 */

#ifndef _TRACKING_H_
#define _TRACKING_H_

#include <string>
using namespace std;

#include "basic_define.h"

/** \class ObjTacking
 *  \brief The implementation of object tracking:
 *         init(), doTrack(), destroy().
 */
class ObjTracking
{
public:
	ObjTracking()
	{
	}
	~ObjTracking()
	{
		destroy();
	}

	/** \fn        init
	 *  \brief     initialize the basic information of the input video and
	 *             necessary parameters.
	 *  \param[IN] w - width of the video frame.
	 *  \param[IN] h - height of the video frame.
	 *  \param[IN] chns_num - number of channels (eg. RGB, chns_num = 3).
	 *  \param[IN] buffer - the data of configure file.
	 *  \param[IN] buffer_len - length of the configure buffer.
	 *  \return    Whether the function run successfully or not.
	 */
	bool init(int w, int h, int chns_num, const char* buffer, int buffer_len);

	/** \fn         doTrack
	 *  \brief      Start to track.
	 *  \param[IN]  frame_data - rgb data of current video frame.
	 *  \return     Whether the function run successfully or not.
	 */
	bool doTrack(const unsigned char* frame_data);

	/** \fn         getTargets
	 *  \brief      To return the tracking results include the
	 *              bounding boxes and rgb data.
	 *  \param[OUT] trasks - tracking results.
	 *  \return     number of targets.
	 */
	int getTargets(TrackList& tracks);

	/** \fn    destroy
	 *  \brief Release recourses.
	 */
	void destroy();

};
// ObjTracking

#endif  // _TRACKING_H_
