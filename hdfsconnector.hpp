/*##############################################################################

 Copyright (C) 2012 HPCC Systems.

 All rights reserved. This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 ############################################################################## */

#include <string>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include "hdfs.h"

using namespace std;

using std::string;
using std::vector;

#define EOL "\n"
#define RETURN_FAILURE -1

class Hdfs_Connector
{
private:

    hdfsFS fs;

 public:
    Hdfs_Connector(const char * hadoopHost, unsigned short int hadoopPort, const char * hadoopUser)
    {
        fs = NULL;
        if (strlen(hadoopUser) > 0)
            fs = hdfsConnectAsUser(hadoopHost, hadoopPort, hadoopUser);
        else
            fs = hdfsConnect(hadoopHost, hadoopPort);

        if (!fs)
        {
            fprintf(stderr, "H2H Error: Could not connect to hdfs on %s:%d\n", hadoopHost, hadoopPort);
        }
    };

    ~Hdfs_Connector()
    {
        if (fs)
            fprintf(stderr, "\nhdfsDisconnect returned: %d\n", hdfsDisconnect(fs));
    };

    hdfsFS getHdfsFS();
    tOffset getBlockSize(const char * filename);
    long getFileSize(const char * filename);
    long getRecordCount(long fsize, int clustersize, int reclen, int nodeid);
    void ouputhosts(const char * rfile);
    void outputFileInfo(hdfsFileInfo * fileInfo);

    int readXMLOffset(const char * filename, unsigned long seekPos, unsigned long readlen, const char * rowTag,
            const char * headerText, const char * footerText, unsigned long bufferSize);
    int readCSVOffset(const char * filename, unsigned long seekPos, unsigned long readlen, const char * eolseq,
            unsigned long bufferSize, bool outputTerminator, unsigned long recLen, unsigned long maxLen,
            const char * quote);
    int readFileOffset(const char * filename, tOffset seekPos, unsigned long readlen, unsigned long bufferSize);
    int streamInFile(const char * rfile, int bufferSize);
    int mergeFile(const char * filename, unsigned nodeid, unsigned clustercount, unsigned bufferSize,
            unsigned flushthreshold, short filereplication, bool deleteparts);
    int writeFlatOffset(const char * filename, unsigned nodeid, unsigned clustercount, const char * fileorpipename);
    void escapedStringToChars(const char * source, string & escaped);

private:
    void getLastXMLElement(string * element, const char * xpath);
    void getLastXPathElement(string * element, const char * xpath);
    void getFirstXPathElement(string * element, const char * xpath);
    void xpath2xml(string * xml, const char * xpath, bool open);
};
