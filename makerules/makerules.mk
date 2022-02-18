SOURCE_URL=https://raw.githubusercontent.com/digital-land/

# deduce the repository
ifeq ($(REPOSITORY),)
REPOSITORY=$(shell basename -s .git `git config --get remote.origin.url`)
endif

.PHONY: \
	init

# keep intermediate files
.SECONDARY:

# don't keep targets build with an error
.DELETE_ON_ERROR:

# work in UTF-8
LANGUAGE := en_GB.UTF-8
LANG := C.UTF-8

# for consistent collation on different machines
LC_COLLATE := C.UTF-8

# current git branch
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

all:: first-pass second-pass

# initialise
init::
	pip install --upgrade pip
ifneq (,$(wildcard requirements.txt))
	pip3 install --upgrade -r requirements.txt
endif
ifneq (,$(wildcard setup.py))
	pip install --upgrade .$(PIP_INSTALL_PACKAGE)
	pip install -e .$(PIP_INSTALL_PACKAGE)
endif

