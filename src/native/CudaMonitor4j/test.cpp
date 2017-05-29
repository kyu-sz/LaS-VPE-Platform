#include <nvml.h>
#include <iostream>

using namespace std;

int main() {
  nvmlReturn_t ret;
  unsigned int devCnt;
  ret = nvmlInit();
  cout << "Init ret=" << ret << endl;
  if (!ret) {
    ret = nvmlDeviceGetCount(&devCnt);
    cout << ret << endl;
    cout << devCnt << endl;
  }
  return 0;
}