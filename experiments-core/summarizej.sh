#!/bin/bash
# $1 - data dir (could be relative or full)
javac summarizej.java
java -classpath . summarizej $1
