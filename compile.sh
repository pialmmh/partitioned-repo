#!/bin/bash

# Compile script for the partitioned repository framework

echo "Compiling partitioned repository framework..."

# Create target directory if it doesn't exist
mkdir -p target/classes

# Compile all Java files at once to handle dependencies
javac -d target/classes src/main/java/com/telcobright/db/**/*.java

if [ $? -eq 0 ]; then
    echo "✅ Compilation successful!"
    echo "   Compiled classes are in: target/classes"
    echo ""
    echo "To run examples:"
    echo "   java -cp target/classes com.telcobright.db.example.SmsMultiTableExample"
    echo "   java -cp target/classes com.telcobright.db.example.OrderPartitionedTableExample"
else
    echo "❌ Compilation failed!"
    exit 1
fi