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
touch result.txt
touch times.txt
java -jar RunKeel.jar > result.txt
grep "Execution Time:" result.txt > times.txt
sublime times.txt
fi