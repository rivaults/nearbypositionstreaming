#!/bin/bash
rm logs/injestion.log
exec 1<&-
exec 2<&-
exec >> logs/injestion.log
exec 2>&1

bash wait-for-it.sh pinot-server:8098 -t 0

/opt/pinot/bin/pinot-admin.sh AddTable -schemaFile in-schema.json -realtimeTableConfigFile in-table.json -controllerHost pinot-controller -exec

/opt/pinot/bin/pinot-admin.sh AddTable -schemaFile out-schema.json -realtimeTableConfigFile out-table.json -controllerHost pinot-controller -exec

