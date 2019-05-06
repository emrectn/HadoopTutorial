#!/bin/bash

# Compile ediyoruz
hadoop com.sun.tools.javac.Main "$1".java
# Jar dosyasi olusturma - cf = "create file"
jar cf "$2".jar "$1"*.class
# Jar icindeki dosyaya erisim
hadoop jar "$2".jar "$1" "$3" "$4" "$5"
