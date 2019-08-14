## General

### Compiler

The project Nebula is developed using C++14, so it requires a compiler supporting C++14 features.

### How to build in Fedora29 or Fedora30
#### dependence

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

#### Build and install 3rd-party Libraries
[Install guide](https://github.com/vesoft-inc/nebula-3rdparty/blob/master/README.md)


#### Build nebula
  - 1. create build dir : `cd nebula && mkdir build && cd build`
  - 2. generate makefile : `cmake -DNEBULA_THIRDPARTY_ROOT=${3rd-party_install_root} ..`
  - 3. make : `make or make -j${threadnum}`
  - 4. install: `make install`
  - notes : the default installation dir is **/usr/local/**, if you want to change the dir, on step 2, your command can be `cmake -DCMAKE_INSTALL_PREFIX=$your_nebula_install_dir ..`


### How to build in Centos7.5, Centos6.5, Ubuntu18.04, Ubuntu16.04

**code url**

- [third-party](https://github.com/vesoft-inc/nebula-3rdparty)
- [nebula](hhttps://github.com/vesoft-inc/nebula)

**dependence package url**

- [centos7.5]()
- [cemtos6.5]()
- [ubuntu16.04/ubuntu18.04]()

#### prepare
Step1: install tool

- **git-lfs**
- **git**

Step2: install deps

**censtos 7.5**

- through yum install deps
	- libtool
	- autoconf
	- autoconf-archive
	- automake
	- perl-WWW-Curl
	- libstdc++-static
	- maven

- download other deps from [centos7.5]()

1) Create a system user, which home directory will be used as a shared file location

```
bash> sudo adduser --system --group --home /home/engshare engshare
```

Please make sure the user's home directory /home/engshare is readable to all users

2) Install all necessary rpm packages in this folder

```
bash > rpm -ivh <rpm file name>
```

3) Modify **~/.bashrc** by appending following lines to the end

```
alias cmake='/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind'

alias ctest='/home/engshare/cmake/bin/ctest'
```
3) Update **~/.bashrc**

```
bash> source ~/.bashrc
```

**censtos 6.5**

- through yum install deps
	- libtool
	- autoconf
	- autoconf-archive
	- automake
	- perl-WWW-Curl
	- perl-YAML
	- perl-CGI
	- glibc-devel
	- libstdc++-static
	- maven

- download other deps from [centos6.5]()

1) Install all necessary rpm packages in this folder

```
bash > sudo rpm -ivh <rpm file name>
```

2) Modify **~/.bashrc** by appending following lines to the end

```
export PATH=/opt/nebula/autoconf/bin:/opt/nebula/automake/bin:/opt/nebula/libtool/bin:/opt/nebula/git/bin:/opt/nebula/gettext/bin:/opt/nebula/flex/bin:/opt/nebula/bison/bin:/opt/nebula/binutils/bin:$PATH
export ACLOCAL_PATH=/opt/nebula/automake/share/aclocal-1.15:/opt/nebula/libtool/share/aclocal:/opt/nebula/autoconf-archive/share/aclocal

alias cmake='/opt/nebula/cmake/bin/cmake -DCMAKE_C_COMPILER=/opt/nebula/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/opt/nebula/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/opt/nebula/gperf/bin -DNEBULA_FLEX_ROOT=/opt/nebula/flex -DNEBULA_BISON_ROOT=/opt/nebula/bison -DNEBULA_BOOST_ROOT=/opt/nebula/boost -DNEBULA_OPENSSL_ROOT=/opt/nebula/openssl -DNEBULA_KRB5_ROOT=/opt/nebula/krb5 -DNEBULA_LIBUNWIND_ROOT=/opt/nebula/libunwind -DNEBULA_READLINE_ROOT=/opt/nebula/readline -DNEBULA_NCURSES_ROOT=/opt/nebula/ncurses'
alias ctest='/opt/nebula/cmake/bin/ctest'
```
3) Update **~/.bashrc**

```
bash> source ~/.bashrc
```

**ubuntu16.04 and ubuntu18.04**

- through yum install deps
	- gcc-multilib
	- libtool
	- autoconf
	- autoconf-archive
	- automake
	- python
	- perl-WWW-Curl
	- maven

- download other deps from [ubuntu]()

1) Create a system user, which home directory will be used as a shared file location

```
bash> sudo adduser --system --group --home /home/engshare engshare
```

Please make sure the user's home directory /home/engshare is readable to all users

2) Install all necessary deb packages in this folder

```
bash > dpkg -i <deb file name>
```

3) Modify **~/.bashrc** by appending following lines to the end

```
alias cmake='/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind -DNEBULA_READLINE_ROOT=/home/engshare/readline -DNEBULA_NCURSES_ROOT=/home/engshare/ncurses'

alias ctest='/home/engshare/cmake/bin/ctest'
```
3) Update **~/.bashrc**

```
bash> source ~/.bashrc
```

#### build third-party code

Step1: **clone nebula-3rdparty**

```
bash> git clone git@github.com:vesoft-inc/nebula-3rdparty.git
```
or

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
```

Step2: **build code**

1) `bash> cd nebula-3rdparty/`

2) `bash> cmake ./`

**if not need to build jave client**

`bash> cmake -DSKIP_JAVE_JAR=OFF ./`

3) `bash> make install`


#### build nebula code

step1: **clone nebula**

```
bash> git clone git@github.com:vesoft-inc/nebula.git
```
or

```
bash> git clone https://github.com/vesoft-inc/nebula.git
```

Step2: **build code**

1) `bash> cd nebula/`

2) `bash> mkdir build && cd build`

3) `bash> cmake ..`

**if not need to build jave client**

`bash> cmake -DSKIP_JAVE_CLENT=ON ..`

4) `bash> make && make install`

#### **Now build finish**


#### notice
error info: **/usr/bin/ld: cannot find Scrt1.o: `No such file or directory`**

**resolve**:

```
bash> export LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:$LIBRARY_PATH
```

error info: **bison verson less than 3.0.5**

**resolve**:

1) download [bison-3.0.5.tar.gz](http://ftp.gnu.org/gnu/bison/bison-3.0.5.tar.gz)

2) build and install

```
bash> ./configure
bash> make && make install

```

error info: **build third-party failed, and _build.log show `No such file or directory`**

**resolve**:

`bash> cd nebula-3rdparty/ && git-lfs pull`


