/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <jni.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <cstdint>
#include "com_vesoft_client_NativeClient.h"

#include "base/Base.h"
#include "dataman/SchemaWriter.h"
#include "dataman/ResultSchemaProvider.h"
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
        jstring clazzType = static_cast<jstring>(env->CallObjectMethod(getClazzObj, getName));

        const char* clazzArray = env->GetStringUTFChars(clazzType, nullptr);
        folly::StringPiece name(clazzArray);
        if (name == "java.lang.Boolean") {
            jmethodID m = env->GetMethodID(clazz, "booleanValue", "()Z");
            bool value = env->CallBooleanMethod(obj, m);
            v.emplace_back(value);
        } else if (name == "java.lang.Integer") {
            jmethodID m = env->GetMethodID(clazz, "intValue", "()I");
            int value = env->CallIntMethod(obj, m);
            v.emplace_back(value);
        } else if (name == "java.lang.Long") {
            jmethodID m = env->GetMethodID(clazz, "longValue", "()J");
            int64_t value = env->CallLongMethod(obj, m);
            v.emplace_back(value);
        } else if (name == "java.lang.Float") {
            jmethodID m = env->GetMethodID(clazz, "floatValue", "()F");
            float value = env->CallFloatMethod(obj, m);
            v.emplace_back(value);
        } else if (name == "java.lang.Double") {
            jmethodID m = env->GetMethodID(clazz, "doubleValue", "()D");
            double value = env->CallDoubleMethod(obj, m);
            v.emplace_back(value);
        } else if (name == "[B") {
            jbyteArray byteArray = reinterpret_cast<jbyteArray>(obj);
            jbyte* b = env->GetByteArrayElements(byteArray, nullptr);
            const char* bytes = reinterpret_cast<const char*>(b);
            int size = env->GetArrayLength(byteArray);
            v.emplace_back(std::string(bytes, size));
        } else {
            // Type Error
            LOG(FATAL) << "Type Error : " << name;
        }
        env->ReleaseStringUTFChars(clazzType, clazzArray);
    }

    nebula::dataman::NebulaCodecImpl codec;
    auto result = codec.encode(v);
    auto *resultArray = reinterpret_cast<const signed char*>(result.data());
    auto resultSize = result.size();

    jbyteArray arrays = env->NewByteArray(resultSize);
    env->SetByteArrayRegion(arrays, 0, resultSize, resultArray);
    return arrays;
}

JNIEXPORT jobject JNICALL Java_com_vesoft_client_NativeClient_decode(JNIEnv *env,
                                                                     jclass clz,
                                                                     jbyteArray encoded,
                                                                     jobjectArray pairs) {
    clz = env->FindClass("com/vesoft/client/NativeClient$Pair");
    jmethodID getField = env->GetMethodID(clz, "getField", "()Ljava/lang/String;");
    jmethodID getClazz = env->GetMethodID(clz, "getClazz", "()Ljava/lang/String;");

    auto len = env->GetArrayLength(pairs);
    nebula::SchemaWriter schemaWriter;
    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring fieldValue = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazzValue = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        const char* fieldArray = env->GetStringUTFChars(fieldValue, nullptr);
        const char* clazzArray = env->GetStringUTFChars(clazzValue, nullptr);
        folly::StringPiece field(fieldArray);
        folly::StringPiece clazz(clazzArray);

        if (clazz == "java.lang.Boolean") {
            schemaWriter.appendCol(std::move(field), nebula::cpp2::SupportedType::BOOL);
        } else if (clazz == "java.lang.Integer") {
            schemaWriter.appendCol(std::move(field), nebula::cpp2::SupportedType::INT);
        } else if (clazz == "java.lang.Long") {
            schemaWriter.appendCol(std::move(field), nebula::cpp2::SupportedType::VID);
        } else if (clazz == "java.lang.Float") {
            schemaWriter.appendCol(std::move(field), nebula::cpp2::SupportedType::FLOAT);
        } else if (clazz == "java.lang.Double") {
            schemaWriter.appendCol(std::move(field), nebula::cpp2::SupportedType::DOUBLE);
        } else if (clazz == "[B") {
            schemaWriter.appendCol(std::move(field), nebula::cpp2::SupportedType::STRING);
        } else {
            // Type Error
            LOG(FATAL) << "Type Error : " << clazz;
        }
        env->ReleaseStringUTFChars(fieldValue, fieldArray);
        env->ReleaseStringUTFChars(clazzValue, clazzArray);
    }
    auto schema = std::make_shared<nebula::ResultSchemaProvider>(schemaWriter.moveSchema());

    jbyte* b = env->GetByteArrayElements(encoded, nullptr);
    const char* bytes = reinterpret_cast<const char*>(b);
    int size = env->GetArrayLength(encoded);
    auto encodedString = std::string(bytes, size);
    nebula::dataman::NebulaCodecImpl codec;
    auto result = codec.decode(std::move(encodedString), schema);
    env->ReleaseByteArrayElements(encoded, b, 0);
    jclass hashMapClazz = env->FindClass("java/util/HashMap");
    jmethodID hashMapInit = env->GetMethodID(hashMapClazz, "<init>", "()V");
    jobject resultObj = env->NewObject(hashMapClazz, hashMapInit, "");

    if (!result.ok()) {
        return resultObj;
    }

    auto resultMap = result.value();
    auto putSign = "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;";
    jmethodID putMethod = env->GetMethodID(hashMapClazz,
                                            "put",
                                             putSign);

    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring fieldValue = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazzValue = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        const char* fieldBytes = env->GetStringUTFChars(fieldValue, nullptr);
        const char* clazzBytes = env->GetStringUTFChars(clazzValue, nullptr);
        folly::StringPiece field(fieldBytes);
        jstring key = env->NewStringUTF(field.toString().c_str());

        folly::StringPiece clazz(clazzBytes);
        if (clazz == "java.lang.Boolean") {
            auto bValue =  boost::any_cast<bool>(resultMap[field.toString()]);
            int valueSize = sizeof(bool);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&bValue));
            env->CallObjectMethod(resultObj, putMethod, key, values);
        } else if (clazz == "java.lang.Integer") {
            auto iValue =  boost::any_cast<int>(resultMap[field.toString()]);
            int valueSize = sizeof(int);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&iValue));
            env->CallObjectMethod(resultObj, putMethod, key, values);
        } else if (clazz == "java.lang.Long") {
            auto lValue =  boost::any_cast<int64_t>(resultMap[field.toString()]);
            int valueSize = sizeof(int64_t);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&lValue));
            env->CallObjectMethod(resultObj, putMethod, key, values);
        } else if (clazz == "java.lang.Float") {
            auto fValue =  boost::any_cast<float>(resultMap[field.toString()]);
            int valueSize = sizeof(float);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&fValue));
            env->CallObjectMethod(resultObj, putMethod, key, values);
        } else if (clazz == "java.lang.Double") {
            auto dValue =  boost::any_cast<double>(resultMap[field.toString()]);
            int valueSize = sizeof(double);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&dValue));
            env->CallObjectMethod(resultObj, putMethod, key, values);
        } else if (clazz == "[B") {
            auto sValue = boost::any_cast<std::string>(resultMap[field.toString()]);
            int valueSize = sValue.size();
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(sValue.c_str()));
            env->CallObjectMethod(resultObj, putMethod, key, values);
        } else {
            // Type Error
            LOG(FATAL) << "Type Error : " << clazz;
            return nullptr;
        }

        env->ReleaseStringUTFChars(fieldValue, fieldBytes);
        env->ReleaseStringUTFChars(clazzValue, clazzBytes);
    }

    return resultObj;
}


