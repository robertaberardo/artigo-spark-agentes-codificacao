#!/bin/bash
# Creates the Python 3.11 dev environment and pre-downloads the Spark jars
# needed for Apache Sedona + S3A (MinIO/S3), so they are already present at
# runtime and Spark does not resolve them via Ivy on each run.
set -ex

# --- Python 3.11 -----------------------------------------------------------
# Fedora 39's default python3 is 3.12, which PySpark 3.5.x does NOT support
# (distutils was removed). Pin 3.11 explicitly.
dnf -y install python3.11 python3.11-devel
dnf clean all

# --- Virtualenv + project deps (from pyproject.toml copied to /docker) -----
python3.11 -m venv /opt/venv
/opt/venv/bin/pip install --upgrade pip setuptools wheel
# [dev] traz o jupyterlab para a exploração de tabelas (exploration/).
/opt/venv/bin/pip install "${DOCKER_RESOURCES:-/docker}[dev]"

# --- Pre-download Spark jars into pyspark's jars dir -----------------------
SPARK_JARS_DIR=$(/opt/venv/bin/python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
echo "Spark jars dir: ${SPARK_JARS_DIR}"

MVN="https://repo1.maven.org/maven2"
download() { wget -q --show-progress -P "${SPARK_JARS_DIR}" "$1"; }

# Apache Sedona 1.9.0 (shaded -> bundles GeoTools/JTS) for Spark 3.5 / Scala 2.12
download "${MVN}/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.9.0/sedona-spark-shaded-3.5_2.12-1.9.0.jar"
download "${MVN}/org/datasyslab/geotools-wrapper/1.9.0-33.5/geotools-wrapper-1.9.0-33.5.jar"

# S3A connector matching PySpark 3.5.4's bundled Hadoop 3.3.4
download "${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download "${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

echo "setup-env.sh complete"
