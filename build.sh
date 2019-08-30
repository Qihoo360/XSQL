#!/bin/bash

sh -x dev/make-distribution.sh --tgz -Phive -Phive-thriftserver -Pyarn -Pxsql | tee build.log
