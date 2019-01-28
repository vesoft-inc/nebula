/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <jni.h>
#include <iostream>
#include <string>
#include <vector>
#include "com_vesoft_client_NativeClient.h"

#include "NebulaCodec.h"
#include "NebulaCodecImpl.h"

JNIEXPORT jstring JNICALL Java_com_vesoft_client_NativeClient_encode(JNIEnv *env,
        jobject obj, jint value) {
  std::vector<boost::any> v;
  v.push_back(value);

  NebulaCodecImpl codec;
  std::string s = codec.encode(v);
  // std::string s = "encoded string";
  return env->NewStringUTF(s.c_str());
}

int main() {
    return 0;
}
