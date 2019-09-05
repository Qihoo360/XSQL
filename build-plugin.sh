#!/usr/bin/env bash
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=512m}"
mvn package -Pxsql -Phive -Phive-thriftserver -Pyarn -DskipTests -Pxsql-plugin -am -pl assembly
VERSION=$(mvn help:evaluate -Dexpression=project.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK_VERSION=$(mvn help:evaluate -Dexpression=spark.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
TGZ_NAME=xsql-assembly_2.11-$VERSION-dist.tgz
FINAL_NAME=xsql-$VERSION-plugin-spark-$SPARK_VERSION.tgz
cd assembly/target
mv $TGZ_NAME ../../$FINAL_NAME
