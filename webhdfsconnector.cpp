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

#include "webhdfsconnector.hpp"

int webhdfsconnector::reachWebHDFS()
{
    int retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not reach WebHDFS\n");
        return retval;
    }

    curl_easy_reset(curl);

    string filestatusstr;

    string getFileStatus;
    getFileStatus.append(baseurl);
    getFileStatus.append("?op=GETFILESTATUS");

    fprintf(stderr, "Testing connection: %s\n", getFileStatus.c_str());
    curl_easy_setopt(curl, CURLOPT_URL, getFileStatus.c_str());
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);

    //Dont' use default curl WRITEFUNCTION bc it outputs value to STDOUT.
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &filestatusstr);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
        retval = EXIT_SUCCESS;
    else
        fprintf(stderr, "Could not reach WebHDFS. Error code: %d\n", res);

    return retval;
}

unsigned long webhdfsconnector::getFileSize(const char * fileurl)
{
    HdfsFileStatus filestat;

    if (getFileStatus(fileurl, &filestat) != RETURN_FAILURE)
        return filestat.length;
    else
        return 0;
}

unsigned long webhdfsconnector::getFileSize()
{
    return targetfilestatus.length;
}

int webhdfsconnector::getFileStatus(const char * fileurl, HdfsFileStatus * filestat)
{
    int retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS\n");
        return retval;
    }

    string filestatusstr;
    string requestheader;

    string getFileStatus;
    getFileStatus.append(fileurl);

    if (hasUserName())
    {
        getFileStatus.append("?user.name=");
        getFileStatus.append(username);
        getFileStatus.append("&op=GETFILESTATUS");
    }
    else
        getFileStatus.append("?op=GETFILESTATUS");

    curl_easy_reset(curl);

    fprintf(stderr, "Retrieving file status: %s\n", getFileStatus.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, getFileStatus.c_str());
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &filestatusstr);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_WRITEHEADER, &requestheader);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
    {
        try
        {
            /*
             * Not using JSON parser to avoid 3rd party deps
             */

            filestat->accessTime = -1;
            filestat->blockSize = -1;
            filestat->group = "";
            filestat->length = -1;
            filestat->modificationTime = -1;
            filestat->owner = "";
            filestat->pathSuffix = "";
            filestat->permission = "";
            filestat->replication = -1;
            filestat->type = "";

            fprintf(stderr, "%s.\n", filestatusstr.c_str());

            if (filestatusstr.find("FileStatus") >=0 )
            {
                int lenpos = filestatusstr.find("length");
                if (lenpos >= 0)
                {
                    int colpos = filestatusstr.find_first_of(':', lenpos);
                    if (colpos > lenpos)
                    {
                        int compos = filestatusstr.find_first_of(',', colpos);
                        if (compos > colpos)
                        {
                            filestat->length = atol(filestatusstr.substr(colpos+1,compos-1).c_str());
                        }
                    }
                }
            }

            retval = EXIT_SUCCESS;
        }
        catch (...) {}

        if (retval != EXIT_SUCCESS)
            fprintf(stderr, "Error fetching HDFS file status.\n");
    }
    else
        fprintf(stderr, "Error fetching HDFS file status. Error code: %d.\n", res);
    return retval;
}

int webhdfsconnector::streamFlatFileOffset(unsigned long seekPos, unsigned long readlen, int maxretries)
{
    int retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS\n");
        return retval;
    }

    curl_easy_reset(curl);
    string readfileurl;
    string requestheader;

    readfileurl.append(targetfileurl).append("?");
    if (hasUserName())
        readfileurl.append("user.name=").append(username).append("&");

    readfileurl.append("op=OPEN&offset=");
    readfileurl.append(template2string(seekPos));
    readfileurl.append("&length=");
    readfileurl.append(template2string(readlen));

    fprintf(stderr, "Reading file data: %s\n", readfileurl.c_str());

    CURLcode res;
    int failed_attempts = 0;
    double dlsize = 0;
    double tottime = 0;
    double dlspeed = 0;

    do
    {
        curl_easy_setopt(curl, CURLOPT_URL, readfileurl.c_str());
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
        curl_easy_setopt(curl, CURLOPT_VERBOSE, true);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS, s_libcurlmaxredirs); //Default as reported by libcurl
                                                       //curl.haxx.se/docs/manpage.html#--max-redirs
                                                       //however the default seems to be 0 in
                                                       //at least one platform (CentOS), therefore
                                                       //Explicitly setting default to 50.

        res = curl_easy_perform(curl);

        if (res == CURLE_OK)
        {
            curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &dlsize);
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &tottime);
            curl_easy_getinfo(curl, CURLINFO_SPEED_DOWNLOAD, &dlspeed);
        }
        else
        {
            failed_attempts++;
            fprintf(stderr, "Error attempting to read from HDFS file: \n\t%s. Error code %d \n", readfileurl.c_str(), res);
        }
    }
    while(res != CURLE_OK && failed_attempts <= maxretries);

    if (res == CURLE_OK)
    {
        retval = EXIT_SUCCESS;
        fprintf(stderr, "\nPipe in FLAT file results:\n");
        fprintf(stderr, "Read time: %.3f secs\t ", tottime);
        fprintf(stderr, "Read speed: %.0f bytes/sec\n", dlspeed);
        fprintf(stderr, "Read size: %.0f bytes\n", dlsize);
    }

    return retval;
}

int webhdfsconnector::streamCSVFileOffset(unsigned long seekPos,
        unsigned long readlen, const char * eolseq, unsigned long bufferSize, bool outputTerminator,
        unsigned long recLen, unsigned long maxLen, const char * quote, int maxretries)
{
    fprintf(stderr, "CSV terminator: \'%s\' and quote: \'%c\'\n", eolseq, quote[0]);
    unsigned long recsFound = 0;

    unsigned eolseqlen = strlen(eolseq);
    if (seekPos > eolseqlen)
        seekPos -= eolseqlen; //read back sizeof(EOL) in case the seekpos happens to be a the first char after an EOL

    bool withinQuote = false;

    string bufferstr ="";
    bufferstr.resize(bufferSize);

    bool stopAtNextEOL = false;
    bool firstEOLfound = seekPos == 0;

    unsigned long currentPos = seekPos;

    fprintf(stderr, "--Start looking: %ld--\n", currentPos);

    unsigned long bytesLeft = readlen;

    const char * buffer;

    curl_easy_reset(curl);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, s_libcurlmaxredirs); //Default as reported by libcurl
                                                   //curl.haxx.se/docs/manpage.html#--max-redirs
                                                   //however the default seems to be 0 in
                                                   //at least one platform (CentOS), therefore
                                                   //Explicitly setting default to 50.
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &bufferstr);

    while(bytesLeft > 0)
    {
        bufferstr.clear();
        double num_read_bytes = readTargetFileOffsetToBuffer(currentPos, bytesLeft > bufferSize ? bufferSize : bytesLeft, maxretries);

        if (num_read_bytes <= 0)
        {
            fprintf(stderr, "\n--Hard Stop at: %ld--\n", currentPos);
            break;
        }

        buffer = bufferstr.c_str();

        for (int bufferIndex = 0; bufferIndex < num_read_bytes; bufferIndex++, currentPos++)
        {
            char currChar = buffer[bufferIndex];

            if (currChar == EOF)
                break;

            if (currChar == quote[0])
            {
                fprintf(stderr, "found quote char at pos: %ld\n", currentPos);
                withinQuote = !withinQuote;
            }

            if (currChar == eolseq[0] && !withinQuote)
            {
                bool eolfound = true;
                double extraNumOfBytesRead = 0;
                string tmpstr("");

                if (eolseqlen > 1)
                {
                    int eoli = bufferIndex;
                    while (eoli < num_read_bytes && eoli - bufferIndex < eolseqlen)
                    {
                        tmpstr.append(1, buffer[eoli++]);
                    }

                    if (eoli == num_read_bytes && tmpstr.size() < eolseqlen)
                    {
                        //looks like we have to do a remote read, but before we do, let's make sure the substring matches
                        if (strncmp(eolseq, tmpstr.c_str(), tmpstr.size())==0)
                        {
                            string tmpbuffer = "";
                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &tmpbuffer);
                            //TODO have to make a read... of eolseqlen - tmpstr.size is it worth it?
                            //read from the current position scanned on the file (currentPos) + number of char looked ahead for eol (eoli - bufferIndex)
                            //up to the necessary chars to determine if we're currently scanning the EOL sequence (eolseqlen - tmpstr.size()

                            extraNumOfBytesRead = readTargetFileOffsetToBuffer(currentPos + (eoli - bufferIndex), eolseqlen - tmpstr.size(), maxretries);
                            for(int y = 0; y < extraNumOfBytesRead; y++)
                                tmpstr.append(1, tmpbuffer.at(y));

                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &bufferstr);
                        }
                    }

                    if (strcmp(tmpstr.c_str(), eolseq) != 0)
                        eolfound = false;
                }

                if (eolfound)
                {
                    if (!firstEOLfound)
                    {
                        bufferIndex = bufferIndex + eolseqlen - 1;
                        currentPos = currentPos + eolseqlen - 1;
                        bytesLeft = bytesLeft - eolseqlen;

                        fprintf(stderr, "\n--Start reading: %ld --\n", currentPos);

                        firstEOLfound = true;
                        continue;
                    }

                    if (outputTerminator)
                    {
                        fprintf(stdout, "%s", eolseq);

                        bufferIndex += eolseqlen;
                        currentPos += eolseqlen;
                        bytesLeft -= eolseqlen;
                    }

                    recsFound++;

                    if (stopAtNextEOL)
                    {
                        fprintf(stderr, "\n--Stop piping: %ld--\n", currentPos);
                        bytesLeft = 0;
                        break;
                    }

                    if (bufferIndex < num_read_bytes)
                        currChar = buffer[bufferIndex];
                    else
                        break;
                }
            }

            //don't pipe until we're beyond the first EOL (if offset = 0 start piping ASAP)
            if (firstEOLfound)
            {
                fprintf(stdout, "%c", currChar);
                bytesLeft--;
            }
            else
            {
                fprintf(stderr, "%c", currChar);
                bytesLeft--;
                if(maxLen > 0 && currentPos-seekPos > maxLen * 10)
                {
                    fprintf(stderr, "\nFirst EOL was not found within the first %ld bytes", currentPos-seekPos);
                    return EXIT_FAILURE;
                }
            }

            if (stopAtNextEOL)
                fprintf(stderr, "%c", currChar);

            // If bytesLeft <= 0 at this point, but he haven't encountered
            // the last EOL, need to continue piping untlil EOL is encountered.
            if (bytesLeft <= 0  && currChar != eolseq[0])
            {
                if(!firstEOLfound)
                {
                    fprintf(stderr, "\n--Reached end of readlen before finding first record start at: %ld (breaking out)--\n",  currentPos);
                    break;
                }

                if (stopAtNextEOL)
                {
                    fprintf(stderr, "Could not find last EOL, breaking out a position %ld\n", currentPos);
                    break;
                }

                fprintf(stderr, "\n--Looking for Last EOL: %ld --\n", currentPos);
                bytesLeft = maxLen; //not sure how much longer until next EOL read up max record len;
                stopAtNextEOL = true;
            }
        }
    }

    fprintf(stderr, "\nCurrentPos: %ld, RecsFound: %ld\n", currentPos, recsFound);

    if (currentPos == seekPos && readlen > 0 )
    {
       fprintf(stderr, "\n--ERROR  0 Bytes Fetched--\n");
       return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

double webhdfsconnector::readTargetFileOffsetToBuffer(unsigned long seekPos, unsigned long readlen, int maxretries)
{
    double returnval = 0;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS");
        return returnval;
    }

    char readfileurl [1024];
    if (hasUserName())
        sprintf(readfileurl, "%s?user.name=%s&op=OPEN&offset=%lu&length=%lu",targetfileurl.c_str(), username.c_str(), seekPos,readlen);
    else
        sprintf(readfileurl, "%s?op=OPEN&offset=%lu&length=%lu",targetfileurl.c_str(), seekPos,readlen);

    curl_easy_setopt(curl, CURLOPT_URL, readfileurl);

    CURLcode res;
    int failed_attempts = 0;
    do
    {
        res = curl_easy_perform(curl);

        if (res == CURLE_OK)
        {
            double dlsize;
            curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &dlsize);
            if (dlsize > readlen)
                fprintf(stderr, "Warning: received incorrect number of bytes from HDFS.\n");
            else
                returnval = dlsize;
        }
        else
        {
            failed_attempts++;
            fprintf(stderr, "Error attempting to read from HDFS file: \n\t%s\n\tError code: %d", readfileurl, res);
        }
    }
    while(res != CURLE_OK && failed_attempts <= maxretries);

    return returnval;
}

unsigned long webhdfsconnector::getTotalFilePartsSize(unsigned clustercount)
{
    unsigned long totalSize = 0;

    for (unsigned node = 0; node < clustercount; node++)
    {
        char filepartname[1024];
        memset(&filepartname[0], 0, sizeof(filepartname));
        sprintf(filepartname,"%s-parts/part_%d_%d", targetfileurl.c_str(), node, clustercount);

        unsigned long partFileSize = getFileSize(filepartname);

        if (partFileSize <= 0)
        {
            fprintf(stderr,"Error: Could not find part file: %s", filepartname);
            return 0;
        }

        totalSize += partFileSize;
    }

    return totalSize;
}

unsigned long webhdfsconnector::appendBufferOffset(long blocksize, short replication, int buffersize, unsigned char * buffer)
{
    //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"

    unsigned long retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS");
        return retval;
    }

    curl_easy_reset(curl);

    char openfileurl [1024];
    if (hasUserName())
        sprintf(openfileurl, "%s?user.name=%s&op=APPEND&buffersize=%d",targetfileurl.c_str(), username.c_str(), buffersize);
    else
        sprintf(openfileurl, "%s?op=APPEND&buffersize=%d",targetfileurl.c_str(), buffersize);

    curl_easy_setopt(curl, CURLOPT_READFUNCTION, continueCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_URL, openfileurl);
    curl_easy_setopt(curl, CURLOPT_POST, true);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, false);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, s_libcurlmaxredirs); //Default as reported by libcurl
                                                   //curl.haxx.se/docs/manpage.html#--max-redirs
                                                   //however the default seems to be 0 in
                                                   //at least one platform (CentOS), therefore
                                                   //Explicitly setting default to 50.

    string header;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl,   CURLOPT_WRITEHEADER, &header);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
    {
        size_t found;
        found = header.find("307 TEMPORARY_REDIRECT");

        if (found!=string::npos)
        {
            found=header.find("Location:",found+22);
            if (found!=string::npos)
            {
                size_t  eolfound =header.find(EOL,found);
                string tmp = header.substr(found+10,eolfound-(found+10) - strlen(EOL));

                curl_easy_setopt(curl, CURLOPT_URL, tmp.c_str());

                curl_easy_setopt(curl, CURLOPT_POST, true);
                curl_easy_setopt(curl, CURLOPT_READFUNCTION, readBufferCallBackCurl);
                curl_easy_setopt(curl, CURLOPT_READDATA, buffer);
                curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, buffersize);
                curl_easy_setopt(curl, CURLOPT_VERBOSE, false);

                header.clear();
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
                curl_easy_setopt(curl,   CURLOPT_WRITEHEADER, &header);

                res = curl_easy_perform(curl);

                fprintf(stderr, "Response: %s\n", header.c_str());
            }
        }
    }

    if (res != CURLE_OK)
        fprintf(stderr, "Error transferring file. Curl error code: %d\n", res);
    else
        retval = buffersize;

    return retval;
}

bool webhdfsconnector::connect ()
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS\n");
        return false;
    }

    baseurl.clear();
    targetfileurl.clear();
    hasusername = false;

    baseurl.append(hadoopHost);
    baseurl.append(":");
    baseurl.append(template2string(hadoopPort));
    baseurl.append(WEBHDFS_VER_PATH);

    targetfileurl.assign(baseurl.c_str());
    baseurl.append("/");
    if (fileName[0] != '/')
        targetfileurl.append("/");

    targetfileurl.append(fileName);

    if (strlen(hdfsuser)>0)
    {
        username.assign(hdfsuser);
        hasusername = true;
    }

    webhdfsreached = (reachWebHDFS() == EXIT_SUCCESS);

    if (webhdfsreached)
        getFileStatus(targetfileurl.c_str(), &targetfilestatus);

    return webhdfsreached;
};

int webhdfsconnector::execute ()
{
    int returnCode = EXIT_FAILURE;

    if (action == HCA_STREAMIN)
    {
       returnCode = streamFileOffset();
    }
    else if (action == HCA_STREAMOUT || action == HCA_STREAMOUTPIPE)
    {
        returnCode = writeFlatOffset();
    }
    else if (action == HCA_MERGEFILE)
    {
        returnCode = mergeFile();
    }
    else
    {
        fprintf(stderr, "\nNo action type detected, exiting.");
    }
    return returnCode;
};

int webhdfsconnector::streamInFile(const char * rfile, int bufferSize)
{
    return 0;
}

int webhdfsconnector::mergeFile()
{
    fprintf(stderr, "Merging of file parts not supported by WEBHDFS version of HDFSConnector!\n");
    return RETURN_FAILURE;
}

int webhdfsconnector::writeFlatOffset()
{
    int retval = RETURN_FAILURE;

     if (strlen(pipepath) <= 0)
     {
         fprintf(stderr, "Bad data pipe or file name found");
         return retval;
     }

     if (!curl)
     {
         fprintf(stderr, "Could not connect to WebHDFS");
         return retval;
     }

     curl_easy_reset(curl);

     char openfileurl [1024];
     if (hasUserName())
         sprintf(openfileurl, "%s-parts/part_%d_%d?user.name=%s&op=CREATE&replication=%d&overwrite=true",targetfileurl.c_str(),nodeID, clusterCount,username.c_str(), 1);
     else
         sprintf(openfileurl, "%s-parts/part_%d_%d?op=CREATE&replication=%d&overwrite=true",targetfileurl.c_str(),nodeID, clusterCount, 1);

     _IO_FILE * datafileorpipe;
     datafileorpipe = fopen(pipepath, "rb");

     fprintf(stderr, "Setting up new HDFS file: %s\n", openfileurl);

     curl_easy_setopt(curl, CURLOPT_READFUNCTION, continueCallBackCurl);
     curl_easy_setopt(curl, CURLOPT_READDATA, datafileorpipe);
     curl_easy_setopt(curl, CURLOPT_URL, openfileurl);
     curl_easy_setopt(curl, CURLOPT_PUT, true);
     curl_easy_setopt(curl, CURLOPT_VERBOSE, true);
     curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, false);

     string header;
     curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
     curl_easy_setopt(curl,   CURLOPT_WRITEHEADER, &header);

     CURLcode res = curl_easy_perform(curl);

     if (res == CURLE_OK)
     {
         size_t found;
         found = header.find_first_of("307 TEMPORARY_REDIRECT");
 //        HTTP/1.1 100 Continue\\r\\n\\r\\nHTTP/1.1 307 TEMPORARY_REDIRECT\\r\\nContent-Type: application/json\\r\\nExpires: Thu, 01-Jan-1970 00:00:00 GMT\\r\\nSet-Cookie: hadoop.auth=\\"u=hadoop&p=hadoop&t=simple&e=1345504538799&s=
         if (found!=string::npos)
         {
             found=header.find("Location:",found+22);
             if (found!=string::npos)
             {
                 size_t  eolfound =header.find(EOL,found);
                 string tmp = header.substr(found+10,eolfound-(found+10) - strlen(EOL));
                 fprintf(stderr, "Redirect location: %s\n", tmp.c_str());

                 curl_easy_setopt(curl, CURLOPT_URL, tmp.c_str());
                 curl_easy_setopt(curl, CURLOPT_UPLOAD, true);
                 curl_easy_setopt(curl, CURLOPT_READFUNCTION, readFileCallBackCurl);
                 curl_easy_setopt(curl, CURLOPT_READDATA, datafileorpipe);

                 string errorbody;
                 curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
                 curl_easy_setopt(curl,   CURLOPT_WRITEDATA, &errorbody);

                 res = curl_easy_perform(curl);
                 if (res == CURLE_OK)
                 {
                     if (errorbody.length() > 0)
                         fprintf(stderr, "Error transferring file: %s\n", errorbody.c_str());
                     else
                         retval = EXIT_SUCCESS;
                 }
                 else
                 {
                     fprintf(stderr, "Error transferring file. Curl error code: %d\n", res);
                 }
             }
         }
     }
     else
     {
         fprintf(stderr, "Error setting up file: %s.\n", openfileurl);
     }

     return retval;
}

int webhdfsconnector::streamFileOffset()
{
    int returnCode = RETURN_FAILURE;

    fprintf(stderr, "\nStreaming in %s...\n", fileName);

    unsigned long fileSize = getFileSize();

    if (fileSize != RETURN_FAILURE)
    {
        if (strcmp(format.c_str(), "FLAT") == 0)
        {
            unsigned long recstoread = getRecordCount(fileSize, clusterCount, recLen, nodeID);
            if (recstoread != RETURN_FAILURE)
            {
                unsigned long totRecsInFile = fileSize / recLen;
                unsigned long offset = nodeID * (totRecsInFile / clusterCount) * recLen;
                unsigned long leftOverRecs = totRecsInFile % clusterCount;

                if (leftOverRecs > 0)
                {
                    if (leftOverRecs > nodeID)
                        offset += nodeID * recLen;
                    else
                        offset += leftOverRecs * recLen;
                }

                fprintf(stderr, "fileSize: %lu offset: %lu size bytes: %lu, recstoread:%lu\n", fileSize, offset,
                        recstoread * recLen, recstoread);

                if (offset < fileSize)
                    returnCode = streamFlatFileOffset(offset, recstoread * recLen, maxRetry);
            }
            else
                fprintf(stderr, "Could not determine number of records to read");
        }

        else if (strcmp(format.c_str(), "CSV") == 0)
        {
            fprintf(stderr, "Filesize: %ld, Offset: %ld, readlen: %ld\n", fileSize,
                    (fileSize / clusterCount) * nodeID, fileSize / clusterCount);

            returnCode = streamCSVFileOffset((fileSize / clusterCount) * nodeID,
                    fileSize / clusterCount, terminator.c_str(), bufferSize, outputTerminator, recLen, maxLen,
                    quote.c_str(), maxRetry);
        }
        else
            fprintf(stderr, "Unknown format type: %s(%s)", format.c_str(), foptions.c_str());
    }
    else
        fprintf(stderr, "Could not determine HDFS file size: %s", fileName);

    return returnCode;
}

long webhdfsconnector::getRecordCount(long fsize, int clustersize, int reclen, int nodeid)
{
    long readSize = fsize / reclen / clustersize;
    if (fsize % reclen)
    {
        fprintf(stderr, "filesize (%lu) not multiple of record length(%d)", fsize, reclen);
        return RETURN_FAILURE;
    }

    if ((fsize / reclen) % clustersize > nodeid)
    {
        readSize++;
        fprintf(stderr, "\nThis node will pipe one extra rec\n");
    }
    return readSize;
}

int main(int argc, char **argv)
{
    int returnCode = EXIT_FAILURE;

    webhdfsconnector * connector = new webhdfsconnector(argc, argv);

    if (connector->connect())
    {
        returnCode = connector->execute();
    }

    if (connector)
        delete (connector);

    return returnCode;
}
