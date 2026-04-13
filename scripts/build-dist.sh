#!/bin/bash


mkdir -p dist
rm -rf dist/*

cargo build --release -p jito-shredstream-proxy

cp target/release/triton-shredproxy dist/triton-shredproxy-ubuntu-22.04
cp target/release/jito-shredstream-proxy dist/jito-shredstream-proxy-ubuntu-22.04