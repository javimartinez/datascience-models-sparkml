#!/usr/bin/env bash

if [ "$#" -ne 1 ];
then
echo 'Name of experiment is mandatory'
exit 0
else
zipextension='.zip'

cd /Users/Javi/development/experimentos_keel
unzip $1${zipextension}
cd $1/scripts
java -jar RunKeel.jar
fi