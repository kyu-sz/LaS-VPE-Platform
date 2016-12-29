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

package org.cripac.isee.vpe.common;

/**
 * Enumeration of data types that can be accepted by applications.
 *
 * Created by ken.yu on 16-10-27.
 */
public enum DataTypes {
    ATTR,
    COMMAND,
    IDRANK,
    TRACKLET,
    TRACKLET_ID,
    TRACKLET_ATTR,
    URL,
    PLAIN_TEXT,
    RAW_VIDEO_FRAG_BYTES,
    /**
     * Login parameters for web cameras.
     *
     * @see LoginParam
     */
    WEBCAM_LOGIN_PARAM,
    NONE
}
