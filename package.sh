#!/bin/bash
DIRS=`ls -l | grep "^d" | awk '{print $9}'`
for d in $DIRS; do
	echo "building $d"
	ZIPFILE="$d.zip"
	cd $d
	zip -r ../$ZIPFILE *
	cd ..
done
