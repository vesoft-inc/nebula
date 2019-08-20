## General

### Compiler

The project Nebula is developed using C++14, so it requires a compiler supporting C++14 features.

### 3rd-party Libraries

The project itself includes the source code of several 3rd-party libraries, which usually does not exist in the OS's public application repositories. In addition to the included libraries, Nebula requires these 3rd-party utilities and libraries to be installed on the system

  - autoconf
  - automake
  - libtool
  - cmake
  - bison
  - unzip
  - boost
  - gperf
  - krb5
  - openssl
  - libunwind
  - ncurses
  - readline
  - bzip2
  - doubleconversion
  - fatal
  - fbthrift
  - folly
  - gflags
  - glog
  - googletest
  - jemalloc
  - libevent
  - mstch
  - proxygen
  - rocksdb
  - snappy
  - wangle
  - zlib
  - zstd

### How to get 3rd-party Libraries

Please refer to [Install guide](https://github.com/vesoft-inc/nebula-3rdparty/blob/master/README.md)

### How to build
  - create build dir: `cd nebula && mkdir build && cd build`
  - generate makefile: `cmake -DNEBULA_THIRDPARTY_ROOT=${3rd-party_install_root} ..`
  - make: `make or make -j${threadnum}`
  - install: `make install`
  - notes : the default installation dir is **/usr/local/**, if you want to change the dir, on step 2, your command can be `cmake -DCMAKE_INSTALL_PREFIX=$your_nebula_install_dir ..`
