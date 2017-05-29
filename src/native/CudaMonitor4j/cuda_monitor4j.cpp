#include <nvml.h>
#include "cuda_monitor4j.h"

inline nvmlDevice_t getDevice(unsigned int index) {
  nvmlDevice_t dev;
  nvmlDeviceGetHandleByIndex(index, &dev);
  return dev;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    initNVML
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_initNVML
    (JNIEnv *env, jobject obj) {
  return nvmlInit();
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getDeviceCount
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getDeviceCount
    (JNIEnv *env, jobject obj) {
  unsigned int cnt;
  nvmlDeviceGetCount(&cnt);
  return cnt;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getFanSpeed
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getFanSpeed
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int fanSpeed;
  nvmlDeviceGetFanSpeed(getDevice((unsigned int) index), &fanSpeed);
  return fanSpeed;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getUtilizationRate
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getUtilizationRate
    (JNIEnv *env, jobject obj, jint index) {
  nvmlUtilization_t rates;
  nvmlDeviceGetUtilizationRates(getDevice((unsigned int) index), &rates);
  return rates.gpu;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getFreeMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getFreeMemory
    (JNIEnv *env, jobject obj, jint index) {
  nvmlMemory_t memory;
  nvmlDeviceGetMemoryInfo(getDevice((unsigned int) index), &memory);
  return (jlong) memory.free;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getTotalMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getTotalMemory
    (JNIEnv *env, jobject obj, jint index) {
  nvmlMemory_t memory;
  nvmlDeviceGetMemoryInfo(getDevice((unsigned int) index), &memory);
  return (jlong) memory.total;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getUsedMemory
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getUsedMemory
    (JNIEnv *env, jobject obj, jint index) {
  nvmlMemory_t memory;
  nvmlDeviceGetMemoryInfo(getDevice((unsigned int) index), &memory);
  return (jlong) memory.used;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getTemperature
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getTemperature
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int temperature;
  nvmlDeviceGetTemperature(getDevice((unsigned int) index), NVML_TEMPERATURE_GPU, &temperature);
  return temperature;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getSlowDownTemperatureThreshold
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getSlowDownTemperatureThreshold
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int thresh;
  nvmlDeviceGetTemperatureThreshold(getDevice((unsigned int) index), NVML_TEMPERATURE_THRESHOLD_SLOWDOWN, &thresh);
  return thresh;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getShutdownTemperatureThreshold
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getShutdownTemperatureThreshold
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int thresh;
  nvmlDeviceGetTemperatureThreshold(getDevice((unsigned int) index), NVML_TEMPERATURE_THRESHOLD_SHUTDOWN, &thresh);
  return thresh;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getPowerLimit
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getPowerLimit
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int limit;
  nvmlDeviceGetEnforcedPowerLimit(getDevice((unsigned int) index), &limit);
  return limit;
}

/*
 * Class:     org_cripac_isee_vpe_ctrl_MonitorThread
 * Method:    getPowerUsage
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_cripac_isee_vpe_ctrl_MonitorThread_getPowerUsage
    (JNIEnv *env, jobject obj, jint index) {
  unsigned int usage;
  nvmlDeviceGetPowerUsage(getDevice((unsigned int) index), &usage);
  return usage;
}