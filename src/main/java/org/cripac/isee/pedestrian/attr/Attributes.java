/*
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
 */

package org.cripac.isee.pedestrian.attr;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.cripac.isee.pedestrian.tracking.Tracklet;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * The Attribute class stores all the pre-defined attributes of a pedestrian at
 * one moment in a track. In other words, each attribute object correspond to
 * one bounding box in a track.
 * <p>
 * <p>
 * TODO Fill the pre-defined attributes. </p>
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class Attributes implements Serializable {

    @SerializedName("tracklet_id")
    public Tracklet.Identifier trackletID;

    @SerializedName("gender_male")
    public float genderMale;

    @SerializedName("gender_female")
    public float genderFemale;

    @SerializedName("gender_other")
    public float genderOther;

    @SerializedName("age_16")
    public float ageSixteen;

    @SerializedName("age_30")
    public float ageThirty;

    @SerializedName("age_45")
    public float ageFortyFive;

    @SerializedName("age_60")
    public float ageSixty;

    @SerializedName("age_older_60")
    public float ageOlderSixty;

    @SerializedName("weight_very_fat")
    public float weightVeryFat;

    @SerializedName("weight_little_fat")
    public float weightLittleFat;

    @SerializedName("weight_normal")
    public float weightNormal;

    @SerializedName("weight_little_thin")
    public float weightLittleThin;

    @SerializedName("weight_very_thin")
    public float weightVeryThin;

    @SerializedName("role_client")
    public float roleClient;

    @SerializedName("role_uniform")
    public float roleUniform;

    @SerializedName("hair_style_null")
    public float hairStyleNull;

    @SerializedName("hair_style_long")
    public float hairStyleLong;

    @SerializedName("head_shoulder_black_hair")
    public float headShoulderBlackHair;

    @SerializedName("head_shoulder_with_hat")
    public float headShoulderWithHat;

    @SerializedName("head_shoulder_glasses")
    public float headShoulderGlasses;

    @SerializedName("head_shoulder_sunglasses")
    public float headShoulderSunglasses;

    @SerializedName("head_shoulder_scarf")
    public float headShoulderScarf;

    @SerializedName("head_shoulder_mask")
    public float headShoulderMask;

    @SerializedName("upper_shirt")
    public float upperShirt;

    @SerializedName("upper_sweater")
    public float upperSweater;

    @SerializedName("upper_vest")
    public float upperVest;

    @SerializedName("upper_tshirt")
    public float upperTshirt;

    @SerializedName("upper_cotton")
    public float upperCotton;

    @SerializedName("upper_jacket")
    public float upperJacket;

    @SerializedName("upper_suit")
    public float upperSuit;

    @SerializedName("upper_hoodie")
    public float upperHoodie;

    @SerializedName("upper_cotta")
    public float upperCotta;

    @SerializedName("upper_other")
    public float upperOther;

    @SerializedName("upper_black")
    public float upperBlack;

    @SerializedName("upper_white")
    public float upperWhite;

    @SerializedName("upper_gray")
    public float upperGray;

    @SerializedName("upper_red")
    public float upperRed;

    @SerializedName("upper_green")
    public float upperGreen;

    @SerializedName("upper_blue")
    public float upperBlue;

    @SerializedName("upper_silvery")
    public float upperSilvery;

    @SerializedName("upper_yellow")
    public float upperYellow;

    @SerializedName("upper_brown")
    public float upperBrown;

    @SerializedName("upper_purple")
    public float upperPurple;

    @SerializedName("upper_pink")
    public float upperPink;

    @SerializedName("upper_orange")
    public float upperOrange;

    @SerializedName("upper_mix_color")
    public float upperMixColor;

    @SerializedName("upper_other_color")
    public float upperOtherColor;

    @SerializedName("lower_pants")
    public float lowerPants;

    @SerializedName("lower_short_pants")
    public float lowerShortPants;

    @SerializedName("lower_skirt")
    public float lowerSkirt;

    @SerializedName("lower_short_skirt")
    public float lowerShortSkirt;

    @SerializedName("lower_long_skirt")
    public float lowerLongSkirt;

    @SerializedName("lower_one_piece")
    public float lowerOnePiece;

    @SerializedName("lower_jean")
    public float lowerJean;

    @SerializedName("lower_tight_pants")
    public float lowerTightPants;

    @SerializedName("lower_black")
    public float lowerBlack;

    @SerializedName("lower_white")
    public float lowerWhite;

    @SerializedName("lower_gray")
    public float lowerGray;

    @SerializedName("lower_red")
    public float lowerRed;

    @SerializedName("lower_green")
    public float lowerGreen;

    @SerializedName("lower_blue")
    public float lowerBlue;

    @SerializedName("lower_silver")
    public float lowerSilver;

    @SerializedName("lower_yellow")
    public float lowerYellow;

    @SerializedName("lower_brown")
    public float lowerBrown;

    @SerializedName("lower_purple")
    public float lowerPurple;

    @SerializedName("lower_pink")
    public float lowerPink;

    @SerializedName("lower_orange")
    public float lowerOrange;

    @SerializedName("lower_mix_color")
    public float lowerMixColor;

    @SerializedName("lower_other_color")
    public float lowerOtherColor;

    @SerializedName("shoes_leather")
    public float shoesLeather;

    @SerializedName("shoes_sport")
    public float shoesSport;

    @SerializedName("shoes_boot")
    public float shoesBoot;

    @SerializedName("shoes_cloth")
    public float shoesCloth;

    @SerializedName("shoes_shandle")
    public float shoesShandle;

    @SerializedName("shoes_casual")
    public float shoesCasual;

    @SerializedName("shoes_other")
    public float shoesOther;

    @SerializedName("shoes_black")
    public float shoesBlack;

    @SerializedName("shoes_white")
    public float shoesWhite;

    @SerializedName("shoes_gray")
    public float shoesGray;

    @SerializedName("shoes_red")
    public float shoesRed;

    @SerializedName("shoes_green")
    public float shoesGreen;

    @SerializedName("shoes_blue")
    public float shoesBlue;

    @SerializedName("shoes_silver")
    public float shoesSilver;

    @SerializedName("shoes_yellow")
    public float shoesYellow;

    @SerializedName("shoes_brown")
    public float shoesBrown;

    @SerializedName("shoes_purple")
    public float shoesPurple;

    @SerializedName("shoes_pink")
    public float shoesPink;

    @SerializedName("shoes_orange")
    public float shoesOrange;

    @SerializedName("shoes_mix_color")
    public float shoesMixColor;

    @SerializedName("shoes_other_color")
    public float shoesOtherColor;

    @SerializedName("accessory_backpack")
    public float accessoryBackpack;

    @SerializedName("accessory_shoulderbag")
    public float accessoryShoulderBag;

    @SerializedName("accessory_handbag")
    public float accessoryHandbag;

    @SerializedName("accessory_waistbag")
    public float accessoryWaistBag;

    @SerializedName("accessory_box")
    public float accessoryBox;

    @SerializedName("accessory_plasticbag")
    public float accessoryPlasticBag;

    @SerializedName("accessory_paperbag")
    public float accessoryPaperBag;

    @SerializedName("accessory_cart")
    public float accessoryCart;

    @SerializedName("accessory_kid")
    public float accessoryKid;

    @SerializedName("accessory_other")
    public float accessoryOther;

    @SerializedName("action_calling")
    public float actionCalling;

    @SerializedName("action_armstretching")
    public float actionArmStretching;

    @SerializedName("action_chatting")
    public float actionChatting;

    @SerializedName("action_gathering")
    public float actionGathering;

    @SerializedName("action_lying")
    public float actionLying;

    @SerializedName("action_crouching")
    public float actionCrouching;

    @SerializedName("action_running")
    public float actionRunning;

    @SerializedName("action_holdthing")
    public float actionHoldThing;

    @SerializedName("action_pushing")
    public float actionPushing;

    @SerializedName("action_pulling")
    public float actionPulling;

    @SerializedName("action_nipthing")
    public float actionNipThing;

    @SerializedName("action_picking")
    public float actionPicking;

    @SerializedName("action_other")
    public float actionOther;

    @SerializedName("view_angle_left")
    public float viewAngleLeft;

    @SerializedName("view_angle_right")
    public float viewAngleRight;

    @SerializedName("view_angle_front")
    public float viewAngleFront;

    @SerializedName("view_angle_back")
    public float viewAngleBack;

    @SerializedName("occlusion_left")
    public float occlusionLeft;

    @SerializedName("occlusion_right")
    public float occlusionRight;

    @SerializedName("occlusion_up")
    public float occlusionUp;

    @SerializedName("occlusion_down")
    public float occlusionDown;

    @SerializedName("occlusion_environment")
    public float occlusionEnvironment;

    @SerializedName("occlusion_accessory")
    public float occlusionAccessory;

    @SerializedName("occlusion_object")
    public float occlusionObject;

    @SerializedName("occlusion_other")
    public float occlusionOther;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Attributes) {
            for (Field field : Attributes.class.getFields()) {
                if (field.getType() == float.class) {
                    try {
                        final float thisValue = (float) field.get(this);
                        final float thatValue = (float) field.get(o);
                        if (thatValue * thisValue < 0 || Math.abs(thisValue - thatValue) >= 0.00001) {
                            System.out.println(field.getName() + ": " + thisValue + " vs " + thatValue + " -> "
                                    + Math.abs(thisValue - thatValue));
                            return false;
                        }
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                        return false;
                    }
                }
            }
            return true;
        } else {
            return super.equals(o);
        }
    }
}
