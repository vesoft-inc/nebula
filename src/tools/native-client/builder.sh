/home/engshare/gcc/bin/g++ -std=c++14 -static-libstdc++ -fPIC \
    -I../.. \
    -I../../common/ \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/linux \
    -shared -o target/libclient.so \
    -L. libdataman.so \
    src/main/cpp/com_vesoft_client_NativeClient.cpp
