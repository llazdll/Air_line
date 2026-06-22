#!/bin/bash
# =============================================================================
# Hadoop + R Environment Configuration
# Airline Delay Analysis — MapReduce Pipeline
# =============================================================================
# Source this file before running any R script in this project:
#   source config/hadoop_env.sh
# =============================================================================

# --- Hadoop Installation Path ---
# Adjust this to your Hadoop installation directory
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}

# --- Hadoop Commands ---
export HADOOP_CMD=$HADOOP_HOME/bin/hadoop
export HADOOP_STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# --- Java Configuration ---
# Adjust to your Java installation. Must match the Java version Hadoop uses.
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}

# --- PATH ---
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH

# --- R Library Path (for RHadoop packages) ---
# If you install packages to a custom location, add it here
export R_LIBS=${R_LIBS:-~/R/library}

# --- rmr2 Backend Options ---
# Set to "hadoop" for cluster mode, "local" for testing without Hadoop
export RMR_BACKEND=${RMR_BACKEND:-hadoop}

echo "============================================"
echo "Hadoop Environment Configured"
echo "============================================"
echo "HADOOP_HOME      = $HADOOP_HOME"
echo "HADOOP_CMD       = $HADOOP_CMD"
echo "HADOOP_STREAMING = $HADOOP_STREAMING"
echo "JAVA_HOME        = $JAVA_HOME"
echo "R_LIBS           = $R_LIBS"
echo "RMR_BACKEND      = $RMR_BACKEND"
echo "============================================"

# Verify Hadoop is accessible
if [ -f "$HADOOP_CMD" ]; then
    echo "Hadoop command found: $($HADOOP_CMD version 2>&1 | head -1)"
else
    echo "WARNING: Hadoop command not found at $HADOOP_CMD"
    echo "Adjust HADOOP_HOME in this file to match your installation."
fi
