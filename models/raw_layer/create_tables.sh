#!/usr/bin/env bash

# run with export GFGH_NAME='YOUR_GFGH_NAME' && bash create_tables.sh

bash create_base.sh > raw_selects/${GFGH_NAME}.sql
# bash create_enabled.sh > matching/per_gfgh/${GFGH_NAME}_enabled_with_sku.sql
# bash create_qs.sh > matching/per_gfgh/${GFGH_NAME}_qs.sql
