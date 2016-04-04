### Enable expected behavior

.DELETE_ON_ERROR:


### Architecture detection

ifneq ($(shell uname -s),Linux)
$(error This code must be built on Linux.)
endif

ifeq ($(shell uname -m),x86_64)
LIBRARY = unit-tests.a
else
LIBRARY = unit-tests32.a
endif


### Other flags

# Language features (C99 etc.)
CFLAGS += -std=c99 -D_DEFAULT_SOURCE -D_POSIX_C_SOURCE=200809L -D_BSD_SOURCE -D_XOPEN_SOURCE -pthread

# Warning and error settings
CFLAGS += -Wall -Wextra

ERROR_FLAGS =  -Werror -Wno-sign-compare -Wno-type-limits
ERROR_FLAGS += -Wno-unused-variable -Wno-unused-function -Wno-unused-label -Wno-unused-parameter -Wno-unused-but-set-variable

# Insert debugging symbols
CFLAGS += -g


### File lists

BINS = mr-wordc mr-sleep mr-trace run-tests
SRCS = mapreduce.c
OBJS = $(addsuffix .o,$(basename $(SRCS)))
MAKEFILES = Makefile.dep Makefile.gcc


### Phony targets

.PHONY: all clean tarball

all: $(BINS)

clean:
	$(RM) $(BINS) $(OBJS) $(MAKEFILES)

tarball: clean
	tar -czf ../project3-$(USER).tar.gz -C.. $(shell basename $(PWD))
	@echo
	@echo Tarball has been created as ../project3-$(USER).tar.gz


### Build rules

$(BINS): LDLIBS+=-lpthread -lrt
$(BINS): $(LIBRARY) mapreduce.o
	$(CC) $(LDFLAGS) -Wl,--defsym=main=$(subst -,_,$@)_main -Wl,--whole-archive $(LIBRARY) -Wl,--no-whole-archive $(filter-out $(LIBRARY),$^) $(LOADLIBES) $(LDLIBS) -o $@

WRAPPED = malloc calloc realloc
COMMA = ,
run-tests: LDFLAGS+=$(addprefix -Wl$(COMMA)--wrap=,$(WRAPPED))

$(filter-out run-tests,$(BINS)): LDFLAGS+=$(foreach sym,$(WRAPPED),-Wl,--defsym=__real_$(sym)=$(sym))

### Automatic dependency generation
Makefile.dep: $(SRCS)
	$(CPP) -MM -MF $@ $^

-include Makefile.dep

### Automatic flag testing
Makefile.gcc:
	for flag in $(ERROR_FLAGS); do \
		$(CC) $$flag -fsyntax-only -xc /dev/null 2>/dev/null && echo "CFLAGS += $$flag" >> $@; \
	done; true

-include Makefile.gcc
