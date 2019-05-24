TARGET  := envelope.exe
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lcrypto -Wl,-Bdynamic -lm -lcurl -lpthread -ldl -lssl -lmysqlclient -llz4
include ../makefile.inc
