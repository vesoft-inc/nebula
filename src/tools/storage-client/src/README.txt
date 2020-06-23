step 1 :
    make nebula_storage_client

step 2 :
    copy NebulaStorageClient.h and NebulaStorageClientValue.h to your development path.
    copy libnebula_storage_client.so to your development path.

step 3 :
    export LD_LIBRARY_PATH=${development_path}:$LD_LIBRARY_PATH

step 4 :
    g++ NebulaStorageClientExample.cpp  -lnebula_storage_client -o test