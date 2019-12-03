/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_vesoft_nebula_NebulaCodec */

#ifndef _Included_com_vesoft_nebula_NebulaCodec
#define _Included_com_vesoft_nebula_NebulaCodec
#ifdef __cplusplus
extern "C" {
#endif
#undef com_vesoft_nebula_NebulaCodec_PARTITION_ID_SIZE
#define com_vesoft_nebula_NebulaCodec_PARTITION_ID_SIZE 4L
#undef com_vesoft_nebula_NebulaCodec_VERTEX_ID_SIZE
#define com_vesoft_nebula_NebulaCodec_VERTEX_ID_SIZE 8L
#undef com_vesoft_nebula_NebulaCodec_TAG_ID_SIZE
#define com_vesoft_nebula_NebulaCodec_TAG_ID_SIZE 4L
#undef com_vesoft_nebula_NebulaCodec_TAG_VERSION_SIZE
#define com_vesoft_nebula_NebulaCodec_TAG_VERSION_SIZE 8L
#undef com_vesoft_nebula_NebulaCodec_EDGE_TYPE_SIZE
#define com_vesoft_nebula_NebulaCodec_EDGE_TYPE_SIZE 4L
#undef com_vesoft_nebula_NebulaCodec_EDGE_RANKING_SIZE
#define com_vesoft_nebula_NebulaCodec_EDGE_RANKING_SIZE 8L
#undef com_vesoft_nebula_NebulaCodec_EDGE_VERSION_SIZE
#define com_vesoft_nebula_NebulaCodec_EDGE_VERSION_SIZE 8L
#undef com_vesoft_nebula_NebulaCodec_VERTEX_SIZE
#define com_vesoft_nebula_NebulaCodec_VERTEX_SIZE 24L
#undef com_vesoft_nebula_NebulaCodec_EDGE_SIZE
#define com_vesoft_nebula_NebulaCodec_EDGE_SIZE 40L
#undef com_vesoft_nebula_NebulaCodec_DATA_KEY_TYPE
#define com_vesoft_nebula_NebulaCodec_DATA_KEY_TYPE 1L
#undef com_vesoft_nebula_NebulaCodec_TAG_MASK
#define com_vesoft_nebula_NebulaCodec_TAG_MASK -1073741825L
#undef com_vesoft_nebula_NebulaCodec_EDGE_MASK
#define com_vesoft_nebula_NebulaCodec_EDGE_MASK 1073741824L
/*
 * Class:     com_vesoft_nebula_NebulaCodec
 * Method:    encode
 * Signature: ([Ljava/lang/Object;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_vesoft_nebula_NebulaCodec_encode
  (JNIEnv *, jclass, jobjectArray);

/*
 * Class:     com_vesoft_nebula_NebulaCodec
 * Method:    decode
 * Signature: ([B[Lcom/vesoft/nebula/NebulaCodec/Pair;)Ljava/util/Map;
 */
JNIEXPORT jobject JNICALL Java_com_vesoft_nebula_NebulaCodec_decode
  (JNIEnv *, jclass, jbyteArray, jobjectArray);

#ifdef __cplusplus
}
#endif
#endif
/* Header for class com_vesoft_nebula_NebulaCodec_Pair */

#ifndef _Included_com_vesoft_nebula_NebulaCodec_Pair
#define _Included_com_vesoft_nebula_NebulaCodec_Pair
#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus
}
#endif
#endif
