FROM fedora:39

# --- Base tooling + Java 11 -----------------------------------------------
# Sedona dropped Java 8 support in 1.8.0, so 1.9.0 requires Java 11+. Java 11
# is the minimal jump from the cluster's Java 8 (headless: no GUI stack).
RUN dnf -y update && dnf -y install \
    wget \
    git \
    nano \
    java-11-openjdk-headless \
    && dnf clean all

ENV JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
ENV PATH=$PATH:$JAVA_HOME/bin

# --- Paths -----------------------------------------------------------------
# spark-project is self-contained: it is BOTH the build context and the runtime
# mount root (mounted at /workspace/spark-project). There is no repo layer above.
ENV PROJECT_PATH=/workspace/spark-project
ENV DOCKER_RESOURCES=/docker

# Spark reads spark-defaults.conf from here (S3A plumbing lives there, mounted
# at runtime). Keeps connection config out of the job code.
ENV SPARK_CONF_DIR=${PROJECT_PATH}/conf

# So jobs can `from utils.column_transforms import ...` (shared functions live
# in utils/, mounted at runtime).
ENV PYTHONPATH=${PROJECT_PATH}

RUN mkdir -p $DOCKER_RESOURCES

# --- GDAL 3.8.1 + PROJ 9.3.1 (cluster script, runs as-is on dnf) -----------
# Built first and in isolation so its expensive ~10-min compile stays cached
# regardless of later dependency (pyproject.toml) changes.
COPY ./docker/resources/install-gdal.sh $DOCKER_RESOURCES/
RUN chmod +x $DOCKER_RESOURCES/install-gdal.sh && $DOCKER_RESOURCES/install-gdal.sh

ENV LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH
ENV PATH=/usr/bin:$PATH
ENV GDAL_DATA=/usr/share/gdal

# --- Python 3.11 venv + project deps + Spark/Sedona/S3A jars ---------------
# Comes after GDAL: editing pyproject.toml only re-runs this layer, not GDAL.
COPY ./pyproject.toml $DOCKER_RESOURCES/
COPY ./docker/resources/setup-env.sh ./docker/resources/start.sh $DOCKER_RESOURCES/
RUN chmod +x $DOCKER_RESOURCES/*.sh && $DOCKER_RESOURCES/setup-env.sh
ENV PATH=/opt/venv/bin:$PATH

# Runtime workdir = a raiz montada do projeto, para `spark-submit validate_setup.py`
# e `spark-submit apps/<app>/jobs/<job>.py` funcionarem com caminhos relativos.
WORKDIR ${PROJECT_PATH}

# Jupyter + Spark UI
EXPOSE 8888
EXPOSE 4040-4045

CMD ["/docker/start.sh"]
