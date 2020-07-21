# Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation; version 2 of the License.

# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
# for more details.

# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# -*- coding: UTF-8 -*-
import os
import subprocess
import re
import time


TESTFAIL = False
matcher1 = re.compile("SET[\s]*\([\s]*ODBC_TESTS")
matcher2 = re.compile(".*\)")
okmatcher = re.compile("^ok [0-9]+")
errmatcher = re.compile("^not ok [0-9]+")


def gettestlist():
    # Parsing CMakeLists.txt for TestCases
    fo = open("CMakeLists.txt", "r")
    teststring = ""
    matched = False

    for line in fo:
        if matched is True:
            teststring += line.strip().replace("\r", "").replace("\n", "")
            if matcher2.match(line):
                break
        if matched is False and matcher1.match(line.strip()):
            teststring += line.strip().replace("\r", "").replace("\n", "")
            matched = True
            if matcher2.match(line):
                break
    testlist = re.findall(r'\"(.*?)\"', teststring)
    return testlist


def runtest(test, command):
    # run single test with given command, log result and print the error case
    timestamp = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
    test_log = os.getcwd() + "\\" + test + "-" + timestamp
    print(test_log)
    with open(test_log + ".txt", 'w+') as f:
        runcommand = "odbc_" + test + ".exe" + command
        ret = subprocess.run(runcommand, shell=True, stdout=f, stderr=f, encoding="gbk", timeout=300)
        if ret.returncode == 0:
            print("TEST SUCCESS.")
        else:
            f.flush()
            f.seek(0, 0)
            output = ""
            for line in f:
                output += line
                if okmatcher.match(line):
                    output = ""
                if errmatcher.match(line):
                    print(output)
                    output = ""
            print("TEST FAILED!!!")
            global TESTFAIL
            TESTFAIL = True


def testall(testlist, options, test_options):
    # run all test in testlist
    os.chdir("./RelWithDebInfo")
    print(os.getcwd())
    print("Running.....")
    num = 1
    for test in testlist:
        if test in test_options:
            command = " -o " + test_options.get(test)
        else:
            command = " -o " + options
        print("Running Test" + str(num) + ": " + test)
        runtest(test, command)
        num += 1

    print("Tests Finished!")
    if TESTFAIL:
        exit(-1)


def main():
    # By default, you should designate a valid DSN using -d option, or test case will fail
    default_options = '-d "Hetu"'
    # For tests need customized options (like a special DSN with certain catalog meet the requirement of test etc),
    # you can add the test name and options here. Remember you should always designate a valid DSN with -d option.
    # Test requirement can be found in each test case's comment.
    # Here are examples as follow
    test_options = {
        "trans": '-d "openLooKeng_Hive" ',
        "connstr": '-d "Hetu" -D "openLooKeng ODBC 1.1 Driver" \
        -u "root" -S "xxx.xxx.xxx.xxx" -P "XXXX" -s "your_catalog"'}

    testlist = gettestlist()
    testall(testlist, default_options, test_options)


if __name__ == '__main__':
    # This script helps you automatically run all openLooKeng ODBC Driver test cases and record the output to log files.
    # You should follow the instruction of BUILD.md to build and install the driver before you run this script.
    main()
