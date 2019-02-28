/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <jni.h>
#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include "com_vesoft_client_NativeClient.h"

#include "dataman/include/NebulaCodec.h"
#include "dataman/NebulaCodecImpl.h"

JNIEXPORT jstring JNICALL Java_com_vesoft_client_NativeClient_encode(JNIEnv *env,
        jobject obj, jobjectArray values) {
    std::vector<boost::any> v;
    jint len = env->GetArrayLength(values);

    for (int i = 0; i < len; i++) {
        obj = env->GetObjectArrayElement(values, i);
        jclass clazz = env->GetObjectClass(obj);
        jmethodID getClazz = env->GetMethodID(clazz, "getClass", "()Ljava/lang/Class;");

        jobject getClazzObj = env->CallObjectMethod(obj, getClazz);
        jclass objClazz = env->GetObjectClass(getClazzObj);
        jmethodID getName = env->GetMethodID(objClazz, "getName", "()Ljava/lang/String;");
        jstring clazz_type = static_cast<jstring>(env->CallObjectMethod(getClazzObj, getName));

        auto name = std::string(env->GetStringUTFChars(clazz_type, NULL));
        if (name.compare("java.lang.Boolean") == 0) {
            jmethodID m = env->GetMethodID(clazz, "booleanValue", "()Z");
            auto value = env->CallBooleanMethod(obj, m);
            v.push_back(value);
        } else if (name.compare("java.lang.Integer") == 0) {
            jmethodID m = env->GetMethodID(clazz, "intValue", "()I");
            auto value = env->CallIntMethod(obj, m);
            v.push_back(value);
        } else if (name.compare("java.lang.Long") == 0) {
            jmethodID m = env->GetMethodID(clazz, "longValue", "()J");
            auto value = env->CallLongMethod(obj, m);
            v.push_back(value);
        } else if (name.compare("java.lang.Float") == 0) {
            jmethodID m = env->GetMethodID(clazz, "floatValue", "()F");
            auto value = env->CallFloatMethod(obj, m);
            v.push_back(value);
        } else if (name.compare("java.lang.Double") == 0) {
            jmethodID m = env->GetMethodID(clazz, "doubleValue", "()D");
            auto value = env->CallDoubleMethod(obj, m);
            v.push_back(value);
        } else if (name.compare("java.lang.String") == 0) {
            jmethodID m = env->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
            jstring j_value = static_cast<jstring>(env->CallObjectMethod(obj, m));
            auto value = std::string(env->GetStringUTFChars(j_value, NULL));
            v.emplace_back(std::move(value));
        } else {
            // Type Error
        }
    }

    nebula::dataman::NebulaCodecImpl codec;
    auto result = codec.encode(v);
    return env->NewString((const jchar *)result.c_str(), result.size());
}
