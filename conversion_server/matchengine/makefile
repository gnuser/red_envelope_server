TARGET  := conversion.exe
INCS = -I ../network -I ../utils -I../libev -I ../depends
LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -L ../libev/.libs  -lev -ljansson -lmpdec -lz -llz4 -lssl   -lcrypto -L ../depends/hiredis -lhiredis -Wl,-Bdynamic -lm -lrdkafka -lpthread -ldl -lcurl -lmysqlclient  -lstdc++
include ../makefile.inc
