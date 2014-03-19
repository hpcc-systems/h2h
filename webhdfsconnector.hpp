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

#include <stdio.h>
#include <string.h>
#include <sstream>
#include <curl/curl.h>

#include "hdfsconnector.hpp"

#define WEBHDFS_VER_PATH "/webhdfs/v1"

static size_t readStringCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode = 0;
    curl_off_t nread;

    if (stream && ((string *)stream)->size() > 0)
    {
        retcode = sprintf((char*)ptr, "%s", ((string *)stream)->c_str());
        ((string *)stream)->clear();
    }

    return retcode;
}

static size_t readBufferCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode = 0;
    curl_off_t nread;

    if (stream && strlen((char *)stream) > 0)
    {
        retcode = sprintf((char*)ptr, "%s", (char *)stream);
        ((char *)stream)[0] = '\0';
    }

    return retcode;
}

static size_t continueCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    return 0;
}

static size_t readFileCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode;
    curl_off_t nread;

    retcode = fread(ptr, size, nmemb, (_IO_FILE *)stream);

  return retcode;
}

static size_t writeToBufferCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    if (stream)
        sprintf((char *)stream, "%s", (char*)ptr);

    return size*nmemb;
}

static size_t writeToStrCallBackCurl( void *ptr, size_t size, size_t nmemb, void *stream)
{
    if (stream)
    {
        ((std::string *)stream)->append((char*)ptr, size*nmemb);
    }
    return size*nmemb;
}

static size_t writeToStdOutCallBackCurl( void *ptr, size_t size, size_t nmemb, void *stream)
{
    fprintf(stdout, "%s", (char*)ptr);
    return size*nmemb;
}

static size_t writeToStdErrCallBackCurl( void *ptr, size_t size, size_t nmemb, void *stream)
{
    fprintf(stderr, "%s", (char*)ptr);
    return size*nmemb;
}

class webhdfsconnector : public hdfsconnector
{
private:
    string baseurl;
    string targetfileurl;
    string username;
    bool hasusername;
    bool webhdfsreached;
    CURL *curl;
    const static short s_libcurlmaxredirs = 50;

    HdfsFileStatus targetfilestatus;

public:

    webhdfsconnector(int argc, char **argv) : hdfsconnector(argc, argv)
    {
        fprintf(stderr, "\nCreating WEBBHDFS based connector.\n");
    }

    ~webhdfsconnector()
    {
        curl_easy_cleanup(curl);
    };

    bool connect ();
    int  execute ();

    int streamInFile(const char * rfile, int bufferSize);
    int mergeFile();
    int writeFlatOffset();
    int streamFileOffset();
    int reachWebHDFS();
    int streamFlatFileOffset(unsigned long seekPos, unsigned long readlen, int maxretries);
    int streamCSVFileOffset(unsigned long seekPos,
            unsigned long readlen, const char * eolseq, unsigned long bufferSize, bool outputTerminator,
            unsigned long recLen, unsigned long maxLen, const char * quote, int maxretries);

    unsigned long getTotalFilePartsSize(unsigned clustercount);

    double readTargetFileOffsetToBuffer(unsigned long seekPos, unsigned long readlen, int maxretries);

    int getFileStatus(const char * fileurl, HdfsFileStatus * filestat);
    unsigned long appendBufferOffset(long blocksize, short replication, int buffersize, unsigned char * buffer);

    unsigned long getFileSize();
    unsigned long getFileSize(const char * url);

    long getRecordCount(long fsize, int clustersize, int reclen, int nodeid);

    bool hasUserName(){return hasusername;}
    bool webHdfsReached(){return webhdfsreached;}
};
