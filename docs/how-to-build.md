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

Currently, we using `git-lfs` to manage the 3rd-party libraries. So make sure `git-lfs` have been installed before building the source code.

Please see [INSTALLING.md](https://github.com/git-lfs/git-lfs/blob/master/INSTALLING.md) for more details.
