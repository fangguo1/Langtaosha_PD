#!/usr/bin/env bash
set -e

CONFIG="local_data/config_storage_server.yaml"
SCRIPT="scripts/import_metadata_db/storage_server_import_json_pubmed.py"

#python3 $SCRIPT --config-path $CONFIG --json-dir /home/wangyuanshi/pubmed_json/11
#python3 $SCRIPT --config-path $CONFIG --json-dir /home/wangyuanshi/pubmed_json/10
python3 $SCRIPT --config-path $CONFIG --json-dir /home/wangyuanshi/pubmed_json/9

