#!/bin/sh
i=0
while [ $i -ne 1000 ]
do
        i=$(($i+2))
        curl -kq -X POST -d "$i" https://localhost:8444/vertices/in
done
