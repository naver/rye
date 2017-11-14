
dnl
dnl BUILD_NUMBER
dnl
define([BUILD_NUMBER_STRING], patsubst(esyscmd([cat ./BUILD_NUMBER]), [
]))dnl
define([MAJOR_VERSION], patsubst(BUILD_NUMBER_STRING, [^\([^.]*\).\([^.]*\).\([^.]*\).*], [\1]))dnl
define([MINOR_VERSION], patsubst(BUILD_NUMBER_STRING, [^\([^.]*\).\([^.]*\).\([^.]*\).*], [\2]))dnl
define([PATCH_VERSION], patsubst(BUILD_NUMBER_STRING, [^\([^.]*\).\([^.]*\).\([^.]*\).*], [\3]))dnl
define([BUILD_SEQ], patsubst(BUILD_NUMBER_STRING, [^\([0-9]*\).\([0-9]*\).\([0-9]*\).0*\([1-9][0-9]*\).*], [\4]))dnl

