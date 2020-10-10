/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "com_vesoft_nebula_NebulaCodec.h"
#include <jni.h>
#include "datamanlite/RowReader.h"
#include "datamanlite/RowWriter.h"
#include "datamanlite/NebulaSchemaProvider.h"

using namespace nebula::dataman::codec;  // NOLINT


JNIEXPORT jbyteArray JNICALL Java_com_vesoft_nebula_NebulaCodec_encode(JNIEnv *env,
                                                                       jclass clazz,
                                                                       jobjectArray values) {
    jint len = env->GetArrayLength(values);
    RowWriter writer;
    for (int i = 0; i < len; i++) {
        jobject obj = env->GetObjectArrayElement(values, i);
        clazz = env->GetObjectClass(obj);
        jmethodID getClazz = env->GetMethodID(clazz, "getClass", "()Ljava/lang/Class;");

        jobject getClazzObj = env->CallObjectMethod(obj, getClazz);
        jclass objClazz = env->GetObjectClass(getClazzObj);
        jmethodID getName = env->GetMethodID(objClazz, "getName", "()Ljava/lang/String;");
        jstring clazzType = static_cast<jstring>(env->CallObjectMethod(getClazzObj, getName));

        const char* clazzArray = env->GetStringUTFChars(clazzType, nullptr);
        Slice name(clazzArray);
        if (name == Slice("java.lang.Boolean")) {
            jmethodID m = env->GetMethodID(clazz, "booleanValue", "()Z");
            bool value = env->CallBooleanMethod(obj, m);
            writer << value;
        } else if (name == Slice("java.lang.Integer")) {
            jmethodID m = env->GetMethodID(clazz, "intValue", "()I");
            int value = env->CallIntMethod(obj, m);
            writer << value;
        } else if (name == Slice("java.lang.Long")) {
            jmethodID m = env->GetMethodID(clazz, "longValue", "()J");
            int64_t value = env->CallLongMethod(obj, m);
            writer << value;
        } else if (name == Slice("java.lang.Float")) {
            jmethodID m = env->GetMethodID(clazz, "floatValue", "()F");
            float value = env->CallFloatMethod(obj, m);
            writer << value;
        } else if (name == Slice("java.lang.Double")) {
            jmethodID m = env->GetMethodID(clazz, "doubleValue", "()D");
            double value = env->CallDoubleMethod(obj, m);
            writer << value;
        } else if (name == Slice("[B")) {
            jbyteArray byteArray = reinterpret_cast<jbyteArray>(obj);
            jbyte* b = env->GetByteArrayElements(byteArray, nullptr);
            const char* bytes = reinterpret_cast<const char*>(b);
            int size = env->GetArrayLength(byteArray);
            writer << Slice(bytes, size);
        } else {
            // Type Error
            LOG(FATAL) << "Type Error : " << name.data();
        }
        env->ReleaseStringUTFChars(clazzType, clazzArray);
    }

    auto result = writer.encode();
    auto *resultArray = reinterpret_cast<const signed char*>(result.data());
    auto resultSize = result.size();

    jbyteArray arrays = env->NewByteArray(resultSize);
    env->SetByteArrayRegion(arrays, 0, resultSize, resultArray);
    return arrays;
}

JNIEXPORT jobject JNICALL Java_com_vesoft_nebula_NebulaCodec_decode(JNIEnv *env,
                                                                    jclass clz,
                                                                    jbyteArray encoded,
                                                                    jobjectArray pairs,
                                                                    jlong version) {
    clz = env->FindClass("com/vesoft/nebula/NebulaCodec$Pair");
    jmethodID getField = env->GetMethodID(clz, "getField", "()Ljava/lang/String;");
    jmethodID getClazz = env->GetMethodID(clz, "getClazz", "()Ljava/lang/String;");

    auto len = env->GetArrayLength(pairs);
    // Now the version is zero, we should use the one passed in.
    auto schema = std::make_shared<NebulaSchemaProvider>(version);
    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring fieldValue = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazzValue = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        const char* fieldArray = env->GetStringUTFChars(fieldValue, nullptr);
        const char* clazzArray = env->GetStringUTFChars(clazzValue, nullptr);
        Slice clazz(clazzArray);
        if (clazz == Slice("java.lang.Boolean")) {
            schema->addField(Slice(fieldArray), ValueType::BOOL);
        } else if (clazz == Slice("java.lang.Integer")) {
            schema->addField(Slice(fieldArray), ValueType::INT);
        } else if (clazz == Slice("java.lang.Long")) {
            schema->addField(Slice(fieldArray), ValueType::INT);
        } else if (clazz == Slice("java.lang.Float")) {
            schema->addField(Slice(fieldArray), ValueType::FLOAT);
        } else if (clazz == Slice("java.lang.Double")) {
            schema->addField(Slice(fieldArray), ValueType::DOUBLE);
        } else if (clazz == Slice("[B")) {
            schema->addField(Slice(fieldArray), ValueType::STRING);
        } else {
            // Type Error
            LOG(FATAL) << "Type Error : " << clazz.data();
        }
        env->ReleaseStringUTFChars(fieldValue, fieldArray);
        env->ReleaseStringUTFChars(clazzValue, clazzArray);
    }

    jclass arrayListClazz = env->FindClass("java/util/ArrayList");
    jmethodID listInit = env->GetMethodID(arrayListClazz, "<init>", "()V");
    jobject list_obj = env->NewObject(arrayListClazz, listInit, "");
    jmethodID list_add = env->GetMethodID(arrayListClazz, "add", "(Ljava/lang/Object;)Z");

    jbyte* b = env->GetByteArrayElements(encoded, nullptr);
    const char* bytes = reinterpret_cast<const char*>(b);
    int size = env->GetArrayLength(encoded);
    auto reader = RowReader::getRowReader(Slice(bytes, size), schema);
    if (reader == nullptr) {
        env->ReleaseByteArrayElements(encoded, b, 0);
        return list_obj;
    }
    for (int i = 0; i < len; i++) {
        jobject o = env->GetObjectArrayElement(pairs, i);
        jstring fieldValue = static_cast<jstring>(env->CallObjectMethod(o, getField));
        jstring clazzValue = static_cast<jstring>(env->CallObjectMethod(o, getClazz));
        const char* fieldBytes = env->GetStringUTFChars(fieldValue, nullptr);
        const char* clazzBytes = env->GetStringUTFChars(clazzValue, nullptr);
        Slice field(fieldBytes);
        Slice clazz(clazzBytes);

        if (clazz == Slice("java.lang.Boolean")) {
            bool bValue;
            auto ret = reader->getBool(i, bValue);
            if (ret != ResultType::SUCCEEDED) {
                break;
            }
            int valueSize = sizeof(bool);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&bValue));
            env->CallBooleanMethod(list_obj, list_add, values);
        } else if (clazz == Slice("java.lang.Integer")) {
            int32_t iValue;
            auto ret = reader->getInt(i, iValue);
            if (ret != ResultType::SUCCEEDED) {
                break;
            }
            int valueSize = sizeof(int32_t);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&iValue));
            env->CallBooleanMethod(list_obj, list_add, values);
        } else if (clazz == Slice("java.lang.Long")) {
            int64_t lValue;
            auto ret = reader->getInt(i, lValue);
            if (ret != ResultType::SUCCEEDED) {
                break;
            }
            int valueSize = sizeof(int64_t);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&lValue));
            env->CallBooleanMethod(list_obj, list_add, values);
        } else if (clazz == Slice("java.lang.Float")) {
            float fValue;
            auto ret = reader->getFloat(i, fValue);
            if (ret != ResultType::SUCCEEDED) {
                break;
            }
            int valueSize = sizeof(float);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&fValue));
            env->CallBooleanMethod(list_obj, list_add, values);
        } else if (clazz == Slice("java.lang.Double")) {
            double dValue;
            auto ret = reader->getDouble(i, dValue);
            if (ret != ResultType::SUCCEEDED) {
                break;
            }
            int valueSize = sizeof(double);
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(&dValue));
            env->CallBooleanMethod(list_obj, list_add, values);
        } else if (clazz == Slice("[B")) {
            Slice sValue;
            auto ret = reader->getString(i, sValue);
            if (ret != ResultType::SUCCEEDED) {
                break;
            }
            int valueSize = sValue.size();
            jbyteArray values = env->NewByteArray(valueSize);
            env->SetByteArrayRegion(values, 0, valueSize,
                                    reinterpret_cast<const signed char*>(sValue.data()));
            env->CallBooleanMethod(list_obj, list_add, values);
        } else {
            // Type Error
            LOG(FATAL) << "Type Error : " << clazz.data();
            return nullptr;
        }

        env->ReleaseStringUTFChars(fieldValue, fieldBytes);
        env->ReleaseStringUTFChars(clazzValue, clazzBytes);
    }

    env->ReleaseByteArrayElements(encoded, b, 0);
    return list_obj;
}


