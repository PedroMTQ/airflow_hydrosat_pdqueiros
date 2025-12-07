#!/bin/bash

# Declare an associative array for temporary environment variables
declare -A TEMP_VAR
export UV_CACHE_DIR="${PWD}/.uv_cache"
echo "Setting UV_CACHE_DIR to: $UV_CACHE_DIR"
mkdir -p "${UV_CACHE_DIR}"

# Extract package name and Python version from pyproject.toml
TEMP_VAR["PACKAGE"]=$(grep -m 1 'name' pyproject.toml | sed -E 's/name = "(.*)"/\1/')
TEMP_VAR["PYTHON_VERSION"]=$(grep -m 1 'python' pyproject.toml | sed -nE 's/.*[~^]=?([0-9]+\.[0-9]+).*/\1/p')
TEMP_VAR["ENV_DIR"]="${PWD}/${TEMP_VAR["PACKAGE"]}"
TEMP_VAR["ENV_PATH"]="${TEMP_VAR["ENV_DIR"]}/bin/activate"
PYTHON_EXECUTABLE="${TEMP_VAR["ENV_DIR"]}/bin/python"

echo "Checking if environment for ${TEMP_VAR["PACKAGE"]} exists..."
if [ -f "${TEMP_VAR["ENV_PATH"]}" ]; then
    echo "${TEMP_VAR["PACKAGE"]} environment already exists. Sourcing ${TEMP_VAR["ENV_PATH"]}."
    source "${TEMP_VAR["ENV_PATH"]}"
    echo "Activated ${TEMP_VAR["PACKAGE"]}."
else
    echo "Environment ${TEMP_VAR["ENV_PATH"]} does NOT exist."
    echo "Creating ${TEMP_VAR["PACKAGE"]} with Python version ${TEMP_VAR["PYTHON_VERSION"]}."
    uv venv "${TEMP_VAR["ENV_DIR"]}" --python "${TEMP_VAR["PYTHON_VERSION"]}"
    TEMP_VAR["ENV_CREATED"]="true"
    source "${TEMP_VAR["ENV_PATH"]}"
    echo "Activated ${TEMP_VAR["PACKAGE"]}."
fi

if ! uv pip install dist/*.whl; then
# if ! uv sync --active; then
    echo "Error: Failed to install ${TEMP_VAR["PACKAGE"]}."
    deactivate
else
    if [ "${TEMP_VAR["ENV_CREATED"]}" = "true" ]; then
        echo "Finished installing ${TEMP_VAR["PACKAGE"]}."
    fi
fi




# Unset the associative array to prevent leaking variables
unset TEMP_VAR

# what will be pushed to the XCOM
echo "$PYTHON_EXECUTABLE"