#!/bin/bash


mkdir -p dist
rm -rf dist/*

cargo build --release --bin triton-shredproxy

mv target/release/triton-shredproxy dist/triton-shredproxy-ubuntu-22.04