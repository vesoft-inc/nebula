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

JNIEXPORT jbyteArray JNICALL Java_com_vesoft_client_NativeClient_encode(JNIEnv *env,
        jclass clazz, jobjectArray values) {
    std::vector<boost::any> v;
    jint len = env->GetArrayLength(values);

    for (int i = 0; i < len; i++) {
        jobject obj = env->GetObjectArrayElement(values, i);
        clazz = env->GetObjectClass(obj);
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
        } else if (name.compare("[B") == 0) {
            char* bytes = reinterpret_cast<char*>(obj);
            auto value = std::string(bytes);
            v.emplace_back(std::move(value));
        } else {
            // Type Error
            std::cout << "Type Error : " << name << std::endl;
        }
    }

    nebula::dataman::NebulaCodecImpl codec;
    auto result = codec.encode(v);
    auto *result_array = reinterpret_cast<const signed char *>(result.data());
    auto result_size = result.size();

    jbyteArray arrays = env->NewByteArray(result_size);
    env->SetByteArrayRegion(arrays, 0, result_size, result_array);
    return arrays;
}
