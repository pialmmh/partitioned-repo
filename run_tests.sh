#!/bin/bash

echo "Running Split-Verse Tests..."
echo "=============================="

# Set classpath
CLASSPATH="target/test-classes:target/classes"
for jar in ~/.m2/repository/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
           ~/.m2/repository/com/zaxxer/HikariCP/5.0.1/HikariCP-5.0.1.jar \
           ~/.m2/repository/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar \
           ~/.m2/repository/org/slf4j/slf4j-simple/2.0.7/slf4j-simple-2.0.7.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# Run Basic Operations Test
echo "Running Basic Operations Test..."
java -cp "$CLASSPATH" com.telcobright.splitverse.tests.SimpleSplitVerseTest

echo ""
echo "Test logs have been generated:"
echo "- SplitVerseBasicOperationsTest_report.log"
echo "- SplitVerseMultiShardTest_report.log"