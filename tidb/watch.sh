#!/bin/bash
tail -f /tmp/cdc_data/tmp.log | awk '/RowChangeEvent/ {print $0; fflush() }' >> out.log