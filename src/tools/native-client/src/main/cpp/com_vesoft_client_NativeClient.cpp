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
                                                                        jclass clazz,
                                                                        jobjectArray values) {
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

        const char *clazz_array = env->GetStringUTFChars(clazz_type, NULL);
        auto name = std::string(clazz_array);
        if (name == "java.lang.Boolean") {
            jmethodID m = env->GetMethodID(clazz, "booleanValue", "()Z");
            bool value = env->CallBooleanMethod(obj, m);
            v.push_back(value);
        } else if (name == "java.lang.Integer") {
            jmethodID m = env->GetMethodID(clazz, "intValue", "()I");
            int value = env->CallIntMethod(obj, m);
            v.push_back(value);
        } else if (name == "java.lang.Long") {
            jmethodID m = env->GetMethodID(clazz, "longValue", "()J");
            auto value = env->CallLongMethod(obj, m);
            v.push_back(value);
        } else if (name == "java.lang.Float") {
            jmethodID m = env->GetMethodID(clazz, "floatValue", "()F");
            float value = env->CallFloatMethod(obj, m);
            v.push_back(value);
        } else if (name == "java.lang.Double") {
            jmethodID m = env->GetMethodID(clazz, "doubleValue", "()D");
            double value = env->CallDoubleMethod(obj, m);
            v.push_back(value);
        } else if (name == "[B") {
            jbyteArray byte_array = reinterpret_cast<jbyteArray>(obj);
            jbyte* b = env->GetByteArrayElements(byte_array, NULL);
            const char* bytes = reinterpret_cast<const char*>(b);
            int size = env->GetArrayLength(byte_array);
            auto value = std::string(bytes, size);
            v.emplace_back(std::move(value));
        } else {
            // Type Error
            LOG(ERROR) << "Type Error : " << name;
        }
        env->ReleaseStringUTFChars(clazz_type, clazz_array);
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
                                                                     jclass clz,
                                                                     jbyteArray encoded,
                                                                     jobjectArray pairs) {
    clz = env->FindClass("com/vesoft/client/NativeClient$Pair");
    jmethodID getField = env->GetMethodID(clz, "getField", "()Ljava/lang/String;");
    jmethodID getClazz = env->GetMethodID(clz, "getClazz", "()Ljava/lang/String;");

    jint len = env->GetArrayLength(pairs);
    std::vector<std::pair<std::string, nebula::cpp2::SupportedType>> fields;
    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring field_value = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazz_value = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        const char* field_byte = env->GetStringUTFChars(field_value, NULL);
        const char* clazz_byte = env->GetStringUTFChars(clazz_value, NULL);
        auto field = std::string(field_byte);
        auto clazz = std::string(clazz_byte);

        if (clazz == "java.lang.Boolean") {
            fields.push_back(std::make_pair(std::move(field), nebula::cpp2::SupportedType::BOOL));
        } else if (clazz == "java.lang.Integer") {
            fields.push_back(std::make_pair(std::move(field), nebula::cpp2::SupportedType::INT));
        } else if (clazz == "java.lang.Long") {
            fields.push_back(std::make_pair(std::move(field), nebula::cpp2::SupportedType::VID));
        } else if (clazz == "java.lang.Float") {
            fields.push_back(std::make_pair(std::move(field), nebula::cpp2::SupportedType::FLOAT));
        } else if (clazz == "java.lang.Double") {
            fields.push_back(std::make_pair(std::move(field), nebula::cpp2::SupportedType::DOUBLE));
        } else if (clazz == "[B") {
            fields.push_back(std::make_pair(std::move(field), nebula::cpp2::SupportedType::STRING));
        } else {
            // Type Error
            LOG(ERROR) << "Type Error : " << clazz;
        }
        env->ReleaseStringUTFChars(field_value, field_byte);
        env->ReleaseStringUTFChars(clazz_value, clazz_byte);
    }

    jbyte* b = env->GetByteArrayElements(encoded, NULL);
    const char* bytes = reinterpret_cast<const char*>(b);
    int size = env->GetArrayLength(encoded);
    auto encoded_string = std::string(bytes, size);
    nebula::dataman::NebulaCodecImpl codec;
    auto result = codec.decode(encoded_string, fields);
    env->ReleaseByteArrayElements(encoded, b, 0);
    jclass hash_map_clazz = env->FindClass("java/util/HashMap");
    jmethodID hash_map_init = env->GetMethodID(hash_map_clazz, "<init>", "()V");
    jobject result_obj = env->NewObject(hash_map_clazz, hash_map_init, "");

    if (!result.ok()) {
        return result_obj;
    }

    auto result_map = result.value();
    auto put_sign = "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;";
    jmethodID put_method = env->GetMethodID(hash_map_clazz,
                                            "put",
                                             put_sign);

    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring field_value = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazz_value = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        const char* field_byte = env->GetStringUTFChars(field_value, NULL);
        const char* clazz_byte = env->GetStringUTFChars(clazz_value, NULL);
        auto field = std::string(field_byte);
        jstring key = env->NewStringUTF(field.c_str());

        auto clazz = std::string(clazz_byte);
        int value_size;

        if (clazz == "java.lang.Boolean") {
            auto b_value =  boost::any_cast<bool>(result_map[field]);
            auto *value = reinterpret_cast<const signed char *>(&b_value);
            value_size = sizeof(bool);
            jbyteArray values = env->NewByteArray(value_size);
            env->SetByteArrayRegion(values, 0, value_size, value);
            env->CallObjectMethod(result_obj, put_method, key, values);
        } else if (clazz == "java.lang.Integer") {
            auto i_value =  boost::any_cast<int>(result_map[field]);
            auto *value = reinterpret_cast<const signed char *>(&i_value);
            value_size = sizeof(int);
            jbyteArray values = env->NewByteArray(value_size);
            env->SetByteArrayRegion(values, 0, value_size, value);
            env->CallObjectMethod(result_obj, put_method, key, values);
        } else if (clazz == "java.lang.Long") {
            auto l_value =  boost::any_cast<int64_t>(result_map[field]);
            auto *value = reinterpret_cast<const signed char *>(&l_value);
            value_size = sizeof(int64_t);
            jbyteArray values = env->NewByteArray(value_size);
            env->SetByteArrayRegion(values, 0, value_size, value);
            env->CallObjectMethod(result_obj, put_method, key, values);
        } else if (clazz == "java.lang.Float") {
            auto f_value =  boost::any_cast<float>(result_map[field]);
            auto *value = reinterpret_cast<const signed char *>(&f_value);
            value_size = sizeof(float);
            jbyteArray values = env->NewByteArray(value_size);
            env->SetByteArrayRegion(values, 0, value_size, value);
            env->CallObjectMethod(result_obj, put_method, key, values);
        } else if (clazz == "java.lang.Double") {
            auto d_value =  boost::any_cast<double>(result_map[field]);
            auto *value = reinterpret_cast<const signed char *>(&d_value);
            value_size = sizeof(double);
            jbyteArray values = env->NewByteArray(value_size);
            env->SetByteArrayRegion(values, 0, value_size, value);
            env->CallObjectMethod(result_obj, put_method, key, values);
        } else if (clazz == "[B") {
            auto s_value = boost::any_cast<std::string>(result_map[field]);
            value_size = s_value.size();
            auto *value = reinterpret_cast<const signed char *>(s_value.c_str());
            jbyteArray values = env->NewByteArray(value_size);
            env->SetByteArrayRegion(values, 0, value_size, value);
            env->CallObjectMethod(result_obj, put_method, key, values);
        } else {
            // Type Error
            LOG(ERROR) << "Type Error : " << clazz;
        }

        env->ReleaseStringUTFChars(field_value, field_byte);
        env->ReleaseStringUTFChars(clazz_value, clazz_byte);
    }

    return result_obj;
}


