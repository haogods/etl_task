#!/usr/bin/env bash

gcc -fPIC mongo.cpp -L /usr/local/lib/ -lmongoc-1.0 -I/usr/local/include/libmongoc-1.0 -I/usr/local/include/libbson-1.0/ -shared -O0 -g -o libmongo.so -lstdc++
mv libmongo.so so/