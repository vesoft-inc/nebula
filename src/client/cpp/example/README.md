How to use cpp client lib
--------------------------------------------

## Option One: Get package from github

### Down package

```bash
wget https://github.com/vesoft-inc/nebula/releases/download/$release-version/nebula-client-$version.$system.x86_64.rpm
```

### Install lib to local

- Centos

```bash
bash> sudo rpm -ivh nebula-client-$version.$system.x86_64.rpm
```

- Ubuntu

```bash
bash> sudo dpkg -i nebula-client-$version.$system.amd64.deb
```

### Update the dynamic library cache

```bash
bash> ldconfig
```

## Option Two: Build by source code

### clone nebula src

```
git clone https://github.com/vesoft-inc/nebula.git
```

### install dependencies

```bash
bash> cd nebula && ./build_dep.sh
```

### build and package

package **RPM**

```bash
bash> cd package
bash> ./package_libcppclient.sh ${VERSION} RPM
```

package **DEB**

```bash
bash> cd package
bash> ./package_libcppclient.sh ${VERSION} DEB
```

after finish package, install the rpm or deb file

## How to use in your code

- Step 1: init arvs(a process can only call once)
- Step 2: init connection pool, the default connections is 10, timeout is 1000 ms (a process can only call once)
- Step 3: create nebula client
- Step 4: auth graph server
- Step 5: execute your statments
- Step 6: disconnect

Please refer to the [sample code](NebulaClientExample.cpp) on detail usage.

### Add libraries to your Makefile
- nebulaClient
- ssl
- bz2
- crypto
- pthread
