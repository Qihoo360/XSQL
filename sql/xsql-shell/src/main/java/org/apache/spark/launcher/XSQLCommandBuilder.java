/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.launcher;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

public class XSQLCommandBuilder extends SparkSubmitCommandBuilder {

  public XSQLCommandBuilder(List<String> args) {
    super(args);
  }

  private void addToClassPath(Set<String> cp, String entries) {
    if (isEmpty(entries)) {
      return;
    }
    String[] split = entries.split(Pattern.quote(File.pathSeparator));
    for (String entry : split) {
      if (!isEmpty(entry)) {
        if (new File(entry).isDirectory() && !entry.endsWith(File.separator)) {
          entry += File.separator;
        }
        cp.add(entry);
      }
    }
  }

  private String getConfDir() {
    String confDir = getenv("SPARK_CONF_DIR");
    return confDir != null ? confDir : join(File.separator, getSparkHome(), "conf");
  }

  @Override
  List<String> buildClassPath(String appClassPath) throws IOException {
    String sparkHome = getSparkHome();

    Set<String> cp = new LinkedHashSet<>();
    addToClassPath(cp, appClassPath);

    addToClassPath(cp, getConfDir());

    List<String> jarToReplace = Arrays.asList(
        "spark-catalyst",
        "spark-sql",
        "spark-hive"
    );
    String jarsDir = findJarsDir(getSparkHome(), getScalaVersion(), true);
    if (jarsDir != null) {
      for (File file: new File(jarsDir).listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          boolean match = false;
          for (String module: jarToReplace) {
            if (name.contains(module)) {
              match = true;
              break;
            }
          }
          return !match;
        }
      })) {
        cp.add(file.getAbsolutePath());
      }
    }
    String xsqlJarsDir = new File(sparkHome, "xsql-jars").getAbsolutePath();
    addToClassPath(cp, join(File.separator, xsqlJarsDir, "*"));

    addToClassPath(cp, getenv("HADOOP_CONF_DIR"));
    addToClassPath(cp, getenv("YARN_CONF_DIR"));
    addToClassPath(cp, getenv("SPARK_DIST_CLASSPATH"));
    return new ArrayList<>(cp);
  }
}
