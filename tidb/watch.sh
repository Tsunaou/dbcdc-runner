#!/bin/bash
echo "Start watching /tmp/cdc_data/tmp.log"
tail -f /tmp/cdc_data/tmp.log | awk '/RowChangeEvent/ {print $0; fflush() }' >> cdc.log
echo "Finish watching /tmp/cdc_data/tmp.log"