#!/bin/bash

# === CONFIGURATION ===
PROJECT_DIR=$(pwd)
DEPLOY_DIR="$HOME/telcobright-projects/partitioned-repo-gh-pages"
VERSION="1.0.0"
GROUP_ID="com.telcobright.db"
ARTIFACT_ID="partitioned-repo"
JAR_FILE="$PROJECT_DIR/target/$ARTIFACT_ID-$VERSION.jar"
POM_FILE="$PROJECT_DIR/pom.xml"

# === BUILD PROJECT ===
echo "Building the project..."
mvn clean install || { echo "Build failed!"; exit 1; }

# === DEPLOY TO GH-PAGES DIR ===
echo "Deploying to GitHub Pages..."
mvn deploy:deploy-file \
  -DgroupId="$GROUP_ID" \
  -DartifactId="$ARTIFACT_ID" \
  -Dversion="$VERSION" \
  -Dpackaging=jar \
  -Dfile="$JAR_FILE" \
  -DpomFile="$POM_FILE" \
  -Durl="file:$DEPLOY_DIR" || { echo "Deploy failed!"; exit 1; }

# === COMMIT AND PUSH TO GH-PAGES ===
cd "$DEPLOY_DIR" || { echo "Failed to enter deploy dir!"; exit 1; }

echo "Committing and pushing to gh-pages..."
git add .
git commit -m "Auto-update $ARTIFACT_ID $VERSION on $(date)"
git push origin gh-pages || { echo "Git push failed!"; exit 1; }

echo "✅ Deployment complete: https://pialmmh.github.io/partitioned-repo/com/telcobright/db/$ARTIFACT_ID/$VERSION/"
