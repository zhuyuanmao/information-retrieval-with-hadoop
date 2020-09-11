#!/bin/bash

javac -cp src/ test/TokenizerTest.java
java -cp src/:test/ TokenizerTest

rm -f src/*.class test/*.class
