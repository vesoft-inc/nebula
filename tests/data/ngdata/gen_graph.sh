#! /usr/bin/env bash

./gdlancer genGraph \
      --max-edges 512 \
      --max-labels 16 \
      --max-props 12 \
      --max-rels 8 \
      --max-vertexes 128 \
      --min-edges 256 \
      --min-labels 12 \
      --min-rels 6 \
      --min-vertexes  64 \
      --num-super-points 1 \
      --seed 1669280622729416000 \
      --format csv
