#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This file contains code from the Apache Spark project (original license above)
# and Delta Lake project (same Apache 2.0 license above).
# It contains modifications, which are licensed as follows:
#

#
#  Copyright (2020) The Hyperspace Project Authors.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import os
import fnmatch
import subprocess
import sys
import shutil
from os import path, environ


def test(root_dir, package):
    # Run all of the test under test/python directory, each of them
    # has main entry point to execute, which is python's unittest testing
    # framework.
    python_root_dir = path.join(root_dir, "python")
    test_dir = path.join(python_root_dir, path.join("hyperspace", "tests"))
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir)
                  if os.path.isfile(os.path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]
    extra_class_path = path.join(python_root_dir, path.join("hyperspace", "testing"))
    for test_file in test_files:
        try:
            if environ.get('SPARK_HOME') == None:
                print("SPARK_HOME is not set in the environment variables.")
                sys.exit(1)
            my_env = os.environ.copy()
            cmd = [os.path.join(my_env["SPARK_HOME"], os.path.join("bin", "spark-submit")),
                   "--driver-class-path=%s" % extra_class_path,
                   "--jars=%s" % extra_class_path,
                   "--packages", package, test_file]
            print("Running tests in %s\n=============" % test_file)
            run_cmd(cmd, stream_output=True, env=my_env)
        except:
            print("Failed tests in %s" % (test_file))
            raise


def delete_if_exists(path):
    # if path exists, delete it.
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)


def prepare(root_dir):
    sbt_path = "sbt"
    delete_if_exists(os.path.expanduser("~/.ivy2/cache/com/microsoft/hyperspace"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/com/microsoft/hyperspace"))
    run_cmd([sbt_path, "project spark2_4", "clean", "publishM2"], stream_output=True)
    
    # Get current release which is required to be loaded
    version = '0.0.0'
    with open(os.path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]
    package = "com.microsoft.hyperspace:hyperspace-core-spark2.4_2.12:" + version
    return package


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


if __name__ == "__main__":
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    package = prepare(root_dir)
    test(root_dir, package)
