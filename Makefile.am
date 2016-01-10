# Copyright (c) 2016 BVK Chaitanya
#
# This file is part of the Ascent Library.
#
# The Ascent Library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# The Ascent Library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with the Ascent Library.  If not, see <http://www.gnu.org/licenses/>.

#
# The primary Make/Automake file for the repository.
#

# Include git repository variables and rules.
include makefiles/Makefile.git

# Users can define custom targets and variables for their personal use by
# defining Makefile.usr file in the parent directory of $(top_srcdir). Some
# variables can be used to influence the build itself, like, for example,
# selecting a different build server, etc.
#
# This file resides outside the git repository and not all engineers may need
# it, so it is completely optional.
-include $(top_srcdir)/../Makefile.usr

# Variables to keep track of build environment.
export BUILD_DATE ?= $(shell date --rfc-3339=ns)
export BUILD_HOST ?= $(shell hostname)
export BUILD_USER ?= $(shell whoami)

# CLEANFILES and CLEANDIRS variables hold names of additional files and
# directories that would be deleted on make-clean.
CLEANFILES =
CLEANDIRS = $(top_srcdir)/bin

clean-local:
	test -z "$(CLEANDIRS)" || rm -rf $(CLEANDIRS)

all-local: proto-all-local go-all-local cc-all-local
check-local: go-check-local cc-check-local

# Make (and Automake) rules specific to each language sources.
include makefiles/Makefile.proto.am
include makefiles/Makefile.go.am
include makefiles/Makefile.cc.am
