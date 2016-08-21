/**\file    basic_define.h
 * \brief   some basic definitions ...
 * \version 0.2
 * \email   da.li@cripac.ia.ac.cn
 */

#ifndef _BASIC_DEFINE_H_
#define _BASIC_DEFINE_H_

#include <vector>
#include <string>
using namespace std;

typedef struct _bb_t
{
	int x;   // Left
	int y;   // Top
	int width;
	int height;
	unsigned char* patch_data;
} BoundingBox;

typedef vector<BoundingBox> BBList;

typedef struct _track_t
{
	int id;
	int tracks_num;
	string video_url;
	int start_frame_idx;
	BBList location_sequence;
} Track;

typedef vector<Track> TrackList;

#endif // _BASIC_DEFINE_H_
