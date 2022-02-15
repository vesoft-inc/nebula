#!/bin/bash
git clone https://github.com/vesoft-inc/nebula-console.git && \
cd nebula-console && make && echo $(pwd) && \
cp nebula-console $(pwd)/../resources/ && ls $(pwd)/../resources/ && \
cd .. && rm -rf nebula-console
