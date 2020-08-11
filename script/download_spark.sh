#!/usr/bin/env bash
#

# 
# Copyright (2020) The Hyperspace Project Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A utility script for build pipeline to download and install spark binaries for 
# python tests to run.

SPARK_VERSION="3.0.0"
HADOOP_VERSION="2.7"
SPARK_DIR="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

curl -k -L -o "spark-${SPARK_VERSION}.tgz" "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIR}.tgz" && tar xzvf "spark-${SPARK_VERSION}.tgz"
