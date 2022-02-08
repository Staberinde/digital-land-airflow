SOURCE_URL=https://raw.githubusercontent.com/digital-land/

# deduce the repository
ifeq ($(REPOSITORY),)
REPOSITORY=$(shell basename -s .git `git config --get remote.origin.url`)
endif

define dataset_url
'https://collection-dataset.s3.eu-west-2.amazonaws.com/$(2)-collection/dataset/$(1).sqlite3'
endef

.PHONY: \
	makerules\
	specification\
	init\
	first-pass\
	second-pass\
	clobber\
	clean\
	commit-makerules\
	prune

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

first-pass::
	@:

# restart the make process to pick-up collected files
second-pass::
	@:

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

