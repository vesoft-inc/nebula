g++ -std=c++11 -fPIC \
    -I../../dataman/include/ \
    -I../../dataman/ \
    -I../../../third-party/folly/_install/include/ \
    -I../../../third-party/double-conversion/_install/include/ \
    -I../../common/ \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/linux \
    -shared -o target/libclient.so \
    -L/tmp/libdataman/ \
    -L../../../third-party/folly/_install/lib/ \
    -L../../../third-party/double-conversion/_install/lib/ \
    src/main/cpp/com_vesoft_client_NativeClient.cpp
