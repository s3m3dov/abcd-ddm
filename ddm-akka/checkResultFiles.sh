#!/bin/bash
echo "Usage: First parameter: File to check; Second parameter: Correct results file."
allINDs=$(wc -l < "$2")
trueINDs=0
foundTrueINDs=0
allINDsFound=false
falseINDs=0
missing=0
 
 while IFS= read -r line; do
    ind=$(grep -F "${line%?}" "$2")
    if [[ -z "$ind" ]]; then
        echo "False: $line"
        ((falseINDs++))
    else 
        echo " True: $ind"
        ((foundTrueINDs++))
    fi
done < "$1"
while IFS= read -r line; do
    ind=$(grep -F "${line%?}" "$1")
    if [[ -n "$ind" ]]; then
        echo "    Found: $ind"
        ((trueINDs++))
    else
        echo "  Missing: $line"
        ((missing++))
    fi
done < "$2"

echo "All: $allINDs, Found: $foundTrueINDs, False: $falseINDs, Missing: $missing"