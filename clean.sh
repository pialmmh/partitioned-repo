#!/bin/bash

# Clean script to remove compiled classes

echo "Cleaning compiled classes..."

# Remove all .class files
find . -name "*.class" -type f -delete

# Remove target directory
rm -rf target/

echo "✅ Clean complete!"