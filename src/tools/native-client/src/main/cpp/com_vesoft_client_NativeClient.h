/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_vesoft_client_NativeClient */

#ifndef _Included_com_vesoft_client_NativeClient
#define _Included_com_vesoft_client_NativeClient
#ifdef __cplusplus
extern "C" {
#endif
#undef com_vesoft_client_NativeClient_PARTITION_ID
#define com_vesoft_client_NativeClient_PARTITION_ID 4L
#undef com_vesoft_client_NativeClient_VERTEX_ID
#define com_vesoft_client_NativeClient_VERTEX_ID 8L
#undef com_vesoft_client_NativeClient_TAG_ID
#define com_vesoft_client_NativeClient_TAG_ID 4L
#undef com_vesoft_client_NativeClient_TAG_VERSION
#define com_vesoft_client_NativeClient_TAG_VERSION 8L
#undef com_vesoft_client_NativeClient_EDGE_TYPE
#define com_vesoft_client_NativeClient_EDGE_TYPE 4L
#undef com_vesoft_client_NativeClient_EDGE_RANKING
#define com_vesoft_client_NativeClient_EDGE_RANKING 8L
#undef com_vesoft_client_NativeClient_EDGE_VERSION
#define com_vesoft_client_NativeClient_EDGE_VERSION 8L
#undef com_vesoft_client_NativeClient_VERTEX_SIZE
#define com_vesoft_client_NativeClient_VERTEX_SIZE 24L
#undef com_vesoft_client_NativeClient_EDGE_SIZE
#define com_vesoft_client_NativeClient_EDGE_SIZE 40L
/*
 * Class:     com_vesoft_client_NativeClient
 * Method:    encode
 * Signature: ([Ljava/lang/Object;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_vesoft_client_NativeClient_encode
  (JNIEnv *, jclass, jobjectArray);

/*
 * Class:     com_vesoft_client_NativeClient
 * Method:    decode
 * Signature: ([B[Lcom/vesoft/client/NativeClient/Pair;)Ljava/util/Map;
 */
JNIEXPORT jobject JNICALL Java_com_vesoft_client_NativeClient_decode
  (JNIEnv *, jclass, jbyteArray, jobjectArray);

#ifdef __cplusplus
}
#endif
#endif
/* Header for class com_vesoft_client_NativeClient_Pair */

#ifndef _Included_com_vesoft_client_NativeClient_Pair
#define _Included_com_vesoft_client_NativeClient_Pair
#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus
}
#endif
#endif
