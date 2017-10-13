#!/bin/sh

RYE=/opt/rye
RYE_DATABASES=$RYE/databases

LIB_PATH=`echo $LD_LIBRARY_PATH | grep -i rye`
if [ "$LIB_PATH" = "" ];
then
	LD_LIBRARY_PATH=$RYE/lib:$LD_LIBRARY_PATH
fi

BIN_PATH=`echo $PATH | grep -i rye`
if [ "$BIN_PATH" = "" ];
then
	PATH=$RYE/bin:$RYE/ryemanager:$PATH
fi

export RYE RYE_DATABASES LD_LIBRARY_PATH PATH
