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

### build

```bash
bash> cd nebula && mkdir build && cd build
bash> cmake -DENABLE_CPPCLIENT_LIB=ON ..
bash> make nebula_client
```

If your g++ can't support c++11, you need clean the build dir and do like this

```bash
bash> cmake -DDISABLE_CXX11_ABI=ON -DENABLE_CPPCLIENT_LIB=ON ..
```


after finish building, cp the lib and include files to your dir

## How to use in your code

- Step 1: init arvs(a process can only call once)
- Step 2: init connection pool, the default connections is 10, timeout is 1000 ms (a process can only call once),

If you want to connect 3 graphd, you can give three graphd's addr and port in init(). For example:

```
    nebula::ConnectionInfo conn1;
    conn1.addr = "192.168.8.11";
    conn1.port = 3699
    nebula::ConnectionInfo conn2;
    conn1.addr = "192.168.8.12";
    conn1.port = 3699
    nebula::ConnectionInfo conn3;
    conn1.addr = "192.168.8.13";
    conn1.port = 3699
    std::vector<nebula::ConnectionInfo> connVec{conn1, conn2, conn3};
    nebula::NebulaClient::initConnectionPool(connVec);

    // create nebula client, using connection pool 1
    nebula::NebulaClient client(conn1.addr, conn1.port);

    // create nebula client, using connection pool 2
    nebula::NebulaClient client(conn2.addr, conn2.port);

    // create nebula client, using connection pool 3
    nebula::NebulaClient client(conn3.addr, conn3.port);
```

- Step 3: create nebula client
- Step 4: auth graph server
- Step 5: execute your statments
- Step 6: disconnect

Please refer to the [sample code](NebulaClientExample.cpp) on detail usage.

build example code
If your g++ version is more than 5.0

```
g++ -std=c++11 NebulaClientExample.cpp -lnebula_client -lpthread -lglog -o nebula_client_example
```

If your g++ version is less than 5.0

```
g++ -std=c++11 NebulaClientExample.cpp -lnebula_client -lpthread -lglog -o nebula_client_example -D ABI_98
```
