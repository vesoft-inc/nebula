### From Source (Linux)

#### Compiling

Click [how to build](docs\manual-EN\1.overview\2.quick-start\3.how-to-build.md) to compile **Nebula Graph**.

#### Running

##### Configure `nebula-metad.conf`

   In your Nebula installation directory (`/usr/local/nebula/`), run

```sh
   > cp etc/nebula-metad.conf.default etc/nebula-metad.conf
```

Modify configurations in `nebula-metad.conf`:

* local_ip
* port
* ws_http_port: metaservice HTTP port
* ws_h2_port: metaservice HTTP2 port
* meta_server_addrs: List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3

##### Configure `nebula-storaged.conf`

```sh
   > cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
```

Modify configurations in `nebula-storaged.conf`:

* port
* ws_http_port: storage service HTTP port
* ws_h2_port: storage service HTTP2 port
* meta_server_addrs: List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3

##### Configure `nebula-graphd.conf`

```sh
   > cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
```

Modify configurations in nebula-graphd.conf:

* local_ip
* port
* ws_http_port: graph service HTTP port
* ws_h2_port: graph service HTTP2 port
* meta_server_addrs: List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3

##### Start Service

```sh
> scripts/nebula.service start all
```

Make sure all the services are working

```sh
> scripts/nebula.service status all
```

##### Connect to Nebula Graph

```sh
> bin/nebula -u=user -p=password --addr={graphd IP address} --port={graphd listening port}
```

* -u is to set the user name, `user` is the default Nebula user account
* -p is to set password, `password` is the default password for account `user`
* --addr is the graphd IP address
* --port is the the graphd server port and the default value is `3699`

Then you’re now ready to start using Nebula Graph.
