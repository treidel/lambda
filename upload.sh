#!/bin/bash
DIRS=`ls -l | grep "^d" | awk '{print $9}'`
for d in $DIRS; do
	echo "uploading $d"
	ZIPFILE="$d.zip"
	aws lambda update-function-code --function-name $d --zip-file fileb://$ZIPFILE 
done
