/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <jni.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <cstdint>
#include "com_vesoft_client_NativeClient.h"

#include "base/Base.h"
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
            auto size = env->GetArrayLength(reinterpret_cast<jbyteArray>(obj));
            auto value = std::string(bytes, size);
            v.emplace_back(std::move(value));
        } else {
            // Type Error
            LOG(ERROR) << "Type Error : " << name;
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

JNIEXPORT jobject JNICALL Java_com_vesoft_client_NativeClient_decode(JNIEnv *env,
                                                                     jobject obj,
                                                                     jstring encoded,
                                                                     jobjectArray pairs) {
    jclass pair_clazz = env->FindClass("com/vesoft/client/NativeClient$Pair");
    jmethodID getField = env->GetMethodID(pair_clazz, "getField", "()Ljava/lang/String;");
    jmethodID getClazz = env->GetMethodID(pair_clazz, "getClazz", "()Ljava/lang/String;");

    jint len = env->GetArrayLength(pairs);
    std::vector<std::pair<std::string, nebula::cpp2::SupportedType>> fields;
    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring field_value = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazz_value = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        auto field = std::string(env->GetStringUTFChars(field_value, NULL));
        auto clazz = std::string(env->GetStringUTFChars(clazz_value, NULL));

        if (clazz.compare("java.lang.Boolean") == 0) {
            fields.push_back(std::make_pair(clazz, nebula::cpp2::SupportedType::BOOL));
        } else if (clazz.compare("java.lang.Integer") == 0) {
            fields.push_back(std::make_pair(clazz, nebula::cpp2::SupportedType::INT));
        } else if (clazz.compare("java.lang.Long") == 0) {
            fields.push_back(std::make_pair(clazz, nebula::cpp2::SupportedType::VID));
        } else if (field.compare("java.lang.Float") == 0) {
            fields.push_back(std::make_pair(field, nebula::cpp2::SupportedType::FLOAT));
        } else if (field.compare("java.lang.Double") == 0) {
            fields.push_back(std::make_pair(clazz, nebula::cpp2::SupportedType::DOUBLE));
        } else if (clazz.compare("java.lang.String") == 0) {
            fields.push_back(std::make_pair(clazz, nebula::cpp2::SupportedType::STRING));
        } else {
            // Type Error
        }
    }

    nebula::dataman::NebulaCodecImpl codec;

    /**
    auto encoded_array = env->GetStringChars(encoded, NULL);
    auto size = static_cast<int>(env->GetStringLength(encoded));
    auto a = env->NewString(encoded_array, size);
    std::cout << size << " " << a << std::endl;

    auto encoded_string = std::string(env->GetStringChars(encoded, NULL));
    **/
    jclass string_clazz = env->FindClass("java/lang/String");
    jmethodID get_bytes = env->GetMethodID(string_clazz, "getBytes", "()[B");
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(encoded, get_bytes);
    jsize size = env->GetArrayLength(byte_array);
    jbyte* bytes = env->GetByteArrayElements(byte_array, JNI_FALSE);

    char* rst = reinterpret_cast<char*>(malloc(size + 1));
    memcpy(rst, bytes, size);
    rst[size] = 0;

    std::string encoded_string(rst);
    auto result = codec.decode(encoded_string, fields);
    if (!result.ok()) {
        return obj;
    }
    auto result_map = result.value();

    /**
    jclass hash_map_clazz = env->FindClass("java/util/HashMap");
    jmethodID hash_map_init = env->GetMethodID(hash_map_clazz, "<init>", "()V");
    auto put_sign = "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;";
    jmethodID put_method = env->GetMethodID(hash_map_clazz,
                                            "put",
                                            put_sign);
                                            **/
    // obj = env->NewObject(hash_map_clazz, hash_map_init);

//    jmethodID string_construction = (env)->GetMethodID(string_clazz,
//                                                       "<init>",
//                                                       "([BLjava/lang/String;)V");
    /**/

    /**/
    for (auto iter = result_map.begin(); iter != result_map.end(); iter++) {
        std::cout << "Result Map " << iter->first << " "  << std::endl;
        // env->CallVoidMethod(obj, put_method, iter->first, iter->second);
    }
    /**/

    /**
    jbyteArray bytes = (env)->NewByteArray(strlen("key"));
    (env)->SetByteArrayRegion(bytes, 0, strlen("key"), (jbyte *)std::string("key").c_str());
    jstring encoding = (env)->NewStringUTF("UTF-8");
    jstring k = (jstring) (env)->NewObject(string_clazz, string_construction, bytes, encoding);
    **/

    // env->CallVoidMethod(obj, put_method, k, k);
    return obj;
}


