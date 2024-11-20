#!/bin/bash

export PYTHONPATH="${PWD%/scripts}:${PYTHONPATH}"

SCRIPT_DIR="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
echo "${SCRIPT_DIR}/frontend/app.py"
streamlit run "${SCRIPT_DIR}/../frontend/app.py"
