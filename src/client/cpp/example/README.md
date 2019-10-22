How to use cpp client lib
--------------------------------------------

### Get lib from github

```
wget https://github.com/vesoft-inc/nebula/releases/download/$release-version/nebula-client-$version.$system.x86_64.rpm
```

### Install lib to local

- Centos

```
bash> sudo rpm -ivh nebula-client-$version.$system.x86_64.rpm
```

- Ubuntu

```
bash> sudo dpkg -i nebula-client-$version.$system.amd64.deb
```
### Add libraries to your Makefile
- nebulaClient
- ssl
- bz2
- crypto
- pthread
