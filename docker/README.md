# Dockerfiles for Nebula Graph Services

**NOTE**: The `Dockerfile.graph` is used to build docker image only for **testing** in local machine since all of nebula services are installed in same image. This is not recommended practice for docker usage.

Following Dockerfiles will be ready in production.

- `Dockerfile.graphd`: nebula-graphd service
- `Dockerfile.metad`: nebula-metad service
- `Dockerfile.storaged`: nebula-storaged service
- `Dockerfile.console`: nebula console client
