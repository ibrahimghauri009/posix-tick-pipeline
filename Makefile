CC      = gcc
CFLAGS  = -Wall -Wextra -pthread -g -I./src
LDFLAGS = -pthread -lrt

SRC     = src
BIN     = .

TARGETS = dispatcher ingester processor reporter

.PHONY: all clean

all: $(TARGETS)

dispatcher: $(SRC)/dispatcher.c $(SRC)/common/common.h
	$(CC) $(CFLAGS) -o $(BIN)/$@ $(SRC)/dispatcher.c $(LDFLAGS)

ingester: $(SRC)/ingester.c $(SRC)/common/common.h
	$(CC) $(CFLAGS) -o $(BIN)/$@ $(SRC)/ingester.c $(LDFLAGS)

processor: $(SRC)/processor.c $(SRC)/common/common.h
	$(CC) $(CFLAGS) -o $(BIN)/$@ $(SRC)/processor.c $(LDFLAGS)

reporter: $(SRC)/reporter.c $(SRC)/common/common.h
	$(CC) $(CFLAGS) -o $(BIN)/$@ $(SRC)/reporter.c $(LDFLAGS)

clean:
	rm -f $(TARGETS)
	rm -f logs/*.log
	rm -f output/report.txt output/report.csv
	rm -f /tmp/os_project_fifo
	@echo "Cleaned."
