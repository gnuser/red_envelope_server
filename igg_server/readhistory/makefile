TARGET  := readhistory.exe
INCS = -I ../network -I ../utils -I ../libev
LIBS = -L ../utils -lutils -L ../network -lnetwork -L ../depends/hiredis -Wl,-Bstatic  -L ../libev/.libs -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lcrypto -lhiredis -lcurl -Wl,-Bdynamic -lm -lpthread -ldl -lssl -lmysqlclient
include ../makefile.inc
