#!/bin/bash

# Get first line from each file
line1=$(head -n 1 "$1")
line2=$(head -n 1 "$2")

echo $line1
echo $line2

# Get the length of the shorter line
len1=${#line1}
len2=${#line2}
max_len=$((len1 > len2 ? len1 : len2))

count=0

echo $len1
echo $len2

# Compare character by character
for ((i=0; i<max_len; i++)); do
    char1="${line1:$i:1}"
    char2="${line2:$i:1}"

    # Check if characters are different
    if [ "$char1" != "$char2" ]; then
        # Check if either character is a number
        if [[ "$char1" =~ [0-9] ]] || [[ "$char2" =~ [0-9] ]]; then
            echo "$i: $char1->$char2"
            exit 1
            ((count++))
        fi
    fi
done

echo "$count"
