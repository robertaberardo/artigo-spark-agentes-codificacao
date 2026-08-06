#!/bin/bash
# Container entrypoint. Ensures the venv + GDAL are on PATH, then keeps the
# container alive so jobs can be run via `docker compose exec` / spark-submit.
set -e

export PATH=/opt/venv/bin:/usr/bin:${PATH}
export LD_LIBRARY_PATH=/usr/lib64:${LD_LIBRARY_PATH}
export GDAL_DATA=/usr/share/gdal

echo "================================================================"
echo " spark-pipelines container ready"
echo "   Spark : $(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null)"
echo "   GDAL  : $(gdalinfo --version 2>/dev/null)"
echo "   Validate the full flow with:"
echo "     spark-submit \$PROJECT_PATH/validate_setup.py"
echo "   Explore tables in Jupyter: bash \$PROJECT_PATH/exploration/start_jupyter.sh"
echo "================================================================"

exec tail -f /dev/null
