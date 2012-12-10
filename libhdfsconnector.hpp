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
#include <iostream>
#include <sstream>
#include <fstream>
#include "hdfs.h"

#include "hdfsconnector.hpp"

class libhdfsconnector : public hdfsconnector
{

private:

    hdfsFS fs;

 public:

    libhdfsconnector(int argc, char **argv) : hdfsconnector(argc, argv)
    {
        fprintf(stderr, "\nCreating LIBHDFS based connector.\n");
    }

    ~libhdfsconnector()
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

    bool connect ();
    int  execute ();

    int readXMLOffset(const char * filename, unsigned long seekPos, unsigned long readlen, const char * rowTag,
            const char * headerText, const char * footerText, unsigned long bufferSize);

    int streamCSVFileOffset(
            const char * filename,
            unsigned long seekPos,
            unsigned long readlen,
            const char * eolseq,
            unsigned long bufferSize,
            bool outputTerminator,
            unsigned long recLen,
            unsigned long maxLen,
            const char * quote,
            int maxretries);

    int streamFlatFileOffset(
            const char * filename,
            unsigned long seekPos,
            unsigned long readlen,
            unsigned long bufferSize,
            int maxretries);

    int streamInFile(const char * rfile, int bufferSize);

    int mergeFile();

    int writeFlatOffset();

    int streamFileOffset();

private:
    void getLastXMLElement(string * element, const char * xpath);
    void getLastXPathElement(string * element, const char * xpath);
    void getFirstXPathElement(string * element, const char * xpath);
    void xpath2xml(string * xml, const char * xpath, bool open);
};
