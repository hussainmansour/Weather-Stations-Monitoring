#!/bin/bash

# Compile first if needed:
# javac -d out Main.java CsvWriter.java

# Call Java program with arguments
if [ "$1" == "--view-all" ]; then
  java -jar target/bitcaskClient-1.0-SNAPSHOT.jar --view-all
elif [ "$1" == "--view" ]; then
  if [[ "$2" == --key=* ]]; then
    java -jar target/bitcaskClient-1.0-SNAPSHOT.jar --view "$2"
  else
    echo "Missing --key=SOME_KEY"
  fi
elif [ "$1" == "--perf" ]; then
  if [[ "$2" == --clients=* ]]; then
    java -jar target/bitcaskClient-1.0-SNAPSHOT.jar --perf "$2"
  else
    echo "Missing --clients=N"
  fi
else
  echo "Usage:"
  echo "  ./bitcask_client.sh --view-all"
  echo "  ./bitcask_client.sh --view --key=SOME_KEY"
  echo "  ./bitcask_client.sh --perf --clients=N"
fi
