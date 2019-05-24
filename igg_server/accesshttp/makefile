TARGET  := igg_http
INCS = -I ../network -I ../utils -I ../libev
LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -L ../libev -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lcrypto -lhiredis -Wl,-Bdynamic -lpthread -lm -ldl
include ../makefile.inc
