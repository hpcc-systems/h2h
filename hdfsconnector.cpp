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

#include "hdfsconnector.hpp"

hdfsFS Hdfs_Connector::getHdfsFS()
{
    return fs;
}

tOffset Hdfs_Connector::getBlockSize(const char * filename)
{
    if (!fs)
    {
        fprintf(stderr, "Could not connect to hdfs");
        return RETURN_FAILURE;
    }

    hdfsFileInfo *fileInfo = NULL;

    if ((fileInfo = hdfsGetPathInfo(fs, filename)) != NULL)
    {
        tOffset bsize = fileInfo->mBlockSize;
        hdfsFreeFileInfo(fileInfo, 1);
        return bsize;
    }
    else
    {
        fprintf(stderr, "Error: hdfsGetPathInfo for %s - FAILED!\n", filename);
        return RETURN_FAILURE;
    }

    return RETURN_FAILURE;
}

long Hdfs_Connector::getFileSize(const char * filename)
{
    if (!fs)
    {
        fprintf(stderr, "Could not connect to hdfs");
        return RETURN_FAILURE;
    }

    hdfsFileInfo *fileInfo = NULL;

    if ((fileInfo = hdfsGetPathInfo(fs, filename)) != NULL)
    {
        long fsize = fileInfo->mSize;
        hdfsFreeFileInfo(fileInfo, 1);
        return fsize;
    }
    else
    {
        fprintf(stderr, "Error: hdfsGetPathInfo for %s - FAILED!\n", filename);
        return RETURN_FAILURE;
    }

    return RETURN_FAILURE;
}

long Hdfs_Connector::getRecordCount(long fsize, int clustersize, int reclen, int nodeid)
{
    long readSize = fsize / reclen / clustersize;
    if (fsize % reclen)
    {
        fprintf(stderr, "filesize (%lu) not multiple of record length(%d)", fsize, reclen);
        return RETURN_FAILURE;
    }
    if ((fsize / reclen) % clustersize >= nodeid + 1)
    {
        readSize += 1;
        fprintf(stderr, "\nThis node will pipe one extra rec\n");
    }
    return readSize;
}

void Hdfs_Connector::ouputhosts(const char * rfile)
{
    if (!fs)
    {
        fprintf(stderr, "Could not connect to hdfs");
        return;
    }

    char*** hosts = hdfsGetHosts(fs, rfile, 1, 1);
    if (hosts)
    {
        fprintf(stderr, "hdfsGetHosts - SUCCESS! ... \n");
        int i = 0;
        while (hosts[i])
        {
            int j = 0;
            while (hosts[i][j])
            {
                fprintf(stderr, "\thosts[%d][%d] - %s\n", i, j, hosts[i][j]);
                ++j;
            }
            ++i;
        }
    }
}

void Hdfs_Connector::outputFileInfo(hdfsFileInfo * fileInfo)
{
    printf("Name: %s, ", fileInfo->mName);
    printf("Type: %c, ", (char) (fileInfo->mKind));
    printf("Replication: %d, ", fileInfo->mReplication);
    printf("BlockSize: %ld, ", fileInfo->mBlockSize);
    printf("Size: %ld, ", fileInfo->mSize);
    printf("LastMod: %s", ctime(&fileInfo->mLastMod));
    printf("Owner: %s, ", fileInfo->mOwner);
    printf("Group: %s, ", fileInfo->mGroup);
    printf("Permissions: %d \n", fileInfo->mPermissions);
}

void Hdfs_Connector::getLastXMLElement(string * element, const char * xpath)
{
    int lasttagclosechar = strlen(xpath) - 1;
    int lasttagopenchar = 0;

    while (lasttagclosechar >= 0)
    {
        if (xpath[lasttagclosechar] == '>')
            break;
        lasttagclosechar--;
    }
    lasttagopenchar = lasttagclosechar;
    while (lasttagopenchar >= 0)
    {
        if (xpath[lasttagopenchar] == '<')
            break;
        lasttagopenchar--;
    }

    element->append(xpath, lasttagopenchar + 1, lasttagclosechar - 1);
}

void Hdfs_Connector::getLastXPathElement(string * element, const char * xpath)
{
    int lastdelimiter = strlen(xpath) - 1;
    while (lastdelimiter >= 0)
    {
        if (xpath[lastdelimiter] == '/')
            break;
        lastdelimiter--;
    }

    element->append(xpath, lastdelimiter + 1, strlen(xpath) - lastdelimiter);
}

void Hdfs_Connector::getFirstXPathElement(string * element, const char * xpath)
{
    int len = strlen(xpath);
    for (int i = 0; i < len; i++)
    {
        element->append(1, xpath[i]);
        if (xpath[i] == '/')
            break;
    }
}

void Hdfs_Connector::xpath2xml(string * xml, const char * xpath, bool open)
{
    vector<string> elements;

    unsigned xpathlen = strlen(xpath);
    for (unsigned i = 0; i < xpathlen;)
    {
        string tmpstr;
        for (; i < strlen(xpath) && xpath[i] != '/';)
            tmpstr.append(1, xpath[i++]);
        i++;
        if (tmpstr.size() > 0)
            elements.push_back(tmpstr);
    }

    if (open)
    {
        for (vector<string>::iterator t = elements.begin(); t != elements.end() - 1; ++t)
            xml->append(1, '<').append(t->c_str()).append(1, '>');
    }
    else
    {
        vector<string>::reverse_iterator rit;
        for (rit = elements.rbegin() + 1; rit < elements.rend(); ++rit)
            xml->append("</").append(rit->c_str()).append(1, '>');
    }
}

int Hdfs_Connector::readXMLOffset(const char * filename, unsigned long seekPos, unsigned long readlen,
        const char * rowTag, const char * headerText, const char * footerText, unsigned long bufferSize)
{
    string xmlizedxpath;
    string elementname;
    string rootelement;
    xpath2xml(&xmlizedxpath, rowTag, true);
    getLastXPathElement(&elementname, rowTag);

    hdfsFile readFile = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
    if (!readFile)
    {
        fprintf(stderr, "Failed to open %s for reading!\n", filename);
        return EXIT_FAILURE;
    }

    if (hdfsSeek(fs, readFile, seekPos))
    {
        fprintf(stderr, "Failed to seek %s for reading!\n", filename);
        return EXIT_FAILURE;
    }

    unsigned char buffer[bufferSize + 1];

    bool firstRowfound = false;

    string openRowTag("<");
    openRowTag.append(elementname).append(1, '>');

    string closeRowTag("</");
    closeRowTag.append(elementname).append(1, '>');

    string closeRootTag("</");
    getLastXMLElement(&closeRootTag, footerText);
    closeRootTag.append(1, '>');

    unsigned long currentPos = seekPos + openRowTag.size();

    string currentTag("");
    bool withinRecord = false;
    bool stopAtNextClosingTag = false;
    bool parsingTag = false;

    fprintf(stderr, "--Start looking <%s>: %ld--\n", elementname.c_str(), currentPos);

    fprintf(stdout, "%s", xmlizedxpath.c_str());

    unsigned long bytesLeft = readlen;
    while (hdfsAvailable(fs, readFile) && bytesLeft > 0)
    {
        tSize numOfBytesRead = hdfsRead(fs, readFile, (void*) buffer, bufferSize);
        if (numOfBytesRead <= 0)
        {
            fprintf(stderr, "\n--Hard Stop at: %ld--\n", currentPos);
            break;
        }

        for (int buffIndex = 0; buffIndex < numOfBytesRead;)
        {
            char currChar = buffer[buffIndex];

            if (currChar == '<' || parsingTag)
            {
                if (!parsingTag)
                    currentTag.clear();

                int tagpos = buffIndex;
                while (tagpos < numOfBytesRead)
                {
                    currentTag.append(1, buffer[tagpos++]);
                    if (buffer[tagpos - 1] == '>')
                        break;
                }

                if (tagpos == numOfBytesRead && buffer[tagpos - 1] != '>')
                {
                    fprintf(stderr, "\nTag accross buffer reads...\n");

                    currentPos += tagpos - buffIndex;
                    bytesLeft -= tagpos - buffIndex;

                    buffIndex = tagpos;
                    parsingTag = true;

                    if (bytesLeft <= 0)
                    {
                        bytesLeft = readlen; //not sure how much longer til next EOL read up readlen;
                        stopAtNextClosingTag = true;
                    }
                    break;
                }
                else
                    parsingTag = false;

                if (!firstRowfound)
                {
                    firstRowfound = strcmp(currentTag.c_str(), openRowTag.c_str()) == 0;
                    if (firstRowfound)
                        fprintf(stderr, "--start piping tag %s at %lu--\n", currentTag.c_str(), currentPos);
                }

                if (strcmp(currentTag.c_str(), closeRootTag.c_str()) == 0)
                {
                    bytesLeft = 0;
                    break;
                }

                if (strcmp(currentTag.c_str(), openRowTag.c_str()) == 0)
                    withinRecord = true;
                else if (strcmp(currentTag.c_str(), closeRowTag.c_str()) == 0)
                    withinRecord = false;
                else if (firstRowfound && !withinRecord)
                {
                    bytesLeft = 0;
                    fprintf(stderr, "Unexpected Tag found: %s at position %lu\n", currentTag.c_str(), currentPos);
                    break;
                }

                currentPos += tagpos - buffIndex;
                bytesLeft -= tagpos - buffIndex;

                buffIndex = tagpos;

                if (bytesLeft <= 0 && !withinRecord)
                    stopAtNextClosingTag = true;

                if (stopAtNextClosingTag && strcmp(currentTag.c_str(), closeRowTag.c_str()) == 0)
                {
                    fprintf(stdout, "%s", currentTag.c_str());
                    fprintf(stderr, "--stop piping at %s %lu--\n", currentTag.c_str(), currentPos);
                    bytesLeft = 0;
                    break;
                }

                if (firstRowfound)
                    fprintf(stdout, "%s", currentTag.c_str());
                else
                    fprintf(stderr, "skipping tag %s\n", currentTag.c_str());

                if (buffIndex < numOfBytesRead)
                    currChar = buffer[buffIndex];
                else
                    break;
            }

            if (firstRowfound)
                fprintf(stdout, "%c", currChar);

            buffIndex++;
            currentPos++;
            bytesLeft--;

            if (bytesLeft <= 0)
            {
                if (withinRecord)
                {
                    fprintf(stderr, "\n--Looking for last closing row tag: %ld--\n", currentPos);
                    bytesLeft = readlen; //not sure how much longer til next EOL read up readlen;
                    stopAtNextClosingTag = true;
                }
                else
                    break;
            }
        }
    }

    xmlizedxpath.clear();

    xpath2xml(&xmlizedxpath, rowTag, false);
    fprintf(stdout, "%s", xmlizedxpath.c_str());

    return EXIT_SUCCESS;
}

int Hdfs_Connector::readCSVOffset(const char * filename, unsigned long seekPos, unsigned long readlen,
        const char * eolseq, unsigned long bufferSize, bool outputTerminator, unsigned long recLen,
        unsigned long maxLen, const char * quote)
{
    fprintf(stderr, "CSV terminator: \'%s\' and quote: \'%c\'\n", eolseq, quote[0]);
    unsigned long recsFound = 0;

    hdfsFile readFile = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
    if (!readFile)
    {
        fprintf(stderr, "Failed to open %s for reading!\n", filename);
        return EXIT_FAILURE;
    }

    unsigned eolseqlen = strlen(eolseq);
    if (seekPos > eolseqlen)
        seekPos -= eolseqlen; //read back sizeof(EOL) in case the seekpos happens to be a the first char after an EOL

    if (hdfsSeek(fs, readFile, seekPos))
    {
        fprintf(stderr, "Failed to seek %s for reading!\n", filename);
        return EXIT_FAILURE;
    }

    bool withinQuote = false;
    unsigned char buffer[bufferSize + 1];

    bool stopAtNextEOL = false;
    bool firstEOLfound = seekPos == 0 ? true : false;

    unsigned long currentPos = seekPos;

    fprintf(stderr, "--Start looking: %ld--\n", currentPos);

    unsigned long bytesLeft = readlen;

    while (hdfsAvailable(fs, readFile) && bytesLeft > 0)
    {
        tSize num_read_bytes = hdfsRead(fs, readFile, (void*) buffer, bufferSize);

        if (num_read_bytes <= 0)
        {
            fprintf(stderr, "\n--Hard Stop at: %ld--\n", currentPos);
            break;
        }
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
                tSize extraNumOfBytesRead = 0;
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
                        if (strncmp(eolseq, tmpstr.c_str(), tmpstr.size()) == 0)
                        {
                            unsigned char tmpbuffer[eolseqlen - tmpstr.size() + 1];
                            //TODO have to make a read... of eolseqlen - tmpstr.size is it worth it?
                            //extraNumOfBytesRead = hdfsRead(*fs, readFile, (void*) tmpbuffer,
                            extraNumOfBytesRead = hdfsRead(fs, readFile, (void*) tmpbuffer, eolseqlen - tmpstr.size());

                            for (int y = 0; y < extraNumOfBytesRead; y++)
                                tmpstr.append(1, tmpbuffer[y]);
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

                        fprintf(stderr, "\n--Start reading: %ld--\n", currentPos);

                        firstEOLfound = true;
                        continue;
                    }

                    if (outputTerminator)
                    {
                        //if (currentPos > seekPos) //Don't output first EOL
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
                else if (extraNumOfBytesRead > 0)
                {
                    if (hdfsSeek(fs, readFile, hdfsTell(fs, readFile) - extraNumOfBytesRead))
                    {
                        fprintf(stderr, "Error while attempting to correct seek position\n");
                        return EXIT_FAILURE;
                    }
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
                if (maxLen > 0 && currentPos - seekPos > maxLen * 10)
                {
                    fprintf(stderr, "\nFirst EOL was not found within the first %lu bytes", currentPos - seekPos);
                    return EXIT_FAILURE;
                }
            }

            if (stopAtNextEOL)
                fprintf(stderr, "%c", currChar);

            // ok, so if bytesLeft <= 0 at this point, we need to keep piping
            // IF the last char read was not an EOL char
            if (bytesLeft <= 0 && currChar != eolseq[0])
            {
                if (!firstEOLfound)
                {
                    fprintf(stderr,
                            "\n--Reached end of readlen before finding first record start at: %ld (breaking out)--\n",
                            currentPos);
                    break;
                }

                fprintf(stderr, "\n--Looking for Last EOL: %ld--\n", currentPos);
                bytesLeft = readlen; //not sure how much longer until next EOL read up readlen;
                stopAtNextEOL = true;
            }
        }
    }

    fprintf(stderr, "\nCurrentPos: %ld, RecsFound: %ld\n", currentPos, recsFound);
    hdfsCloseFile(fs, readFile);

    return EXIT_SUCCESS;
}

int Hdfs_Connector::readFileOffset(const char * filename, tOffset seekPos, unsigned long readlen,
        unsigned long bufferSize)
{
    hdfsFile readFile = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
    if (!readFile)
    {
        fprintf(stderr, "Failed to open %s for reading!\n", filename);
        return EXIT_FAILURE;
    }

    if (hdfsSeek(fs, readFile, seekPos))
    {
        fprintf(stderr, "Failed to seek %s for reading!\n", filename);
        return EXIT_FAILURE;
    }

    unsigned char buffer[bufferSize + 1];

    unsigned long currentPos = seekPos;

    fprintf(stderr, "\n--Start piping: %ld--\n", currentPos);

    unsigned long bytesLeft = readlen;
    while (hdfsAvailable(fs, readFile) && bytesLeft > 0)
    {
        tSize num_read_bytes = hdfsRead(fs, readFile, buffer, bytesLeft < bufferSize ? bytesLeft : bufferSize);
        if (num_read_bytes <= 0)
            break;
        bytesLeft -= num_read_bytes;
        for (int i = 0; i < num_read_bytes; i++, currentPos++)
            fprintf(stdout, "%c", buffer[i]);
    }

    fprintf(stderr, "--\nStop Streaming: %ld--\n", currentPos);

    hdfsCloseFile(fs, readFile);

    return EXIT_SUCCESS;
}

int Hdfs_Connector::streamInFile(const char * rfile, int bufferSize)
{
    if (!fs)
    {
        fprintf(stderr, "Could not connect to hdfs on");
        return RETURN_FAILURE;
    }

    unsigned long fileTotalSize = 0;

    hdfsFileInfo *fileInfo = NULL;
    if ((fileInfo = hdfsGetPathInfo(fs, rfile)) != NULL)
    {
        fileTotalSize = fileInfo->mSize;
        hdfsFreeFileInfo(fileInfo, 1);
    }
    else
    {
        fprintf(stderr, "Error: hdfsGetPathInfo for %s - FAILED!\n", rfile);
        return RETURN_FAILURE;
    }

    hdfsFile readFile = hdfsOpenFile(fs, rfile, O_RDONLY, bufferSize, 0, 0);
    if (!readFile)
    {
        fprintf(stderr, "Failed to open %s for writing!\n", rfile);
        return RETURN_FAILURE;
    }

    unsigned char buff[bufferSize + 1];
    buff[bufferSize] = '\0';

    for (unsigned long bytes_read = 0; bytes_read < fileTotalSize;)
    {
        unsigned long read_length = hdfsRead(fs, readFile, buff, bufferSize);
        bytes_read += read_length;
        for (unsigned long i = 0; i < read_length; i++)
            fprintf(stdout, "%c", buff[i]);
    }

    hdfsCloseFile(fs, readFile);

    return 0;
}

int Hdfs_Connector::mergeFile(const char * filename, unsigned nodeid, unsigned clustercount, unsigned bufferSize,
        unsigned flushthreshold, short filereplication, bool deleteparts)
{
    if (nodeid == 0)
    {
        if (!fs)
        {
            fprintf(stderr, "Could not connect to hdfs on");
            return RETURN_FAILURE;
        }

        fprintf(stderr, "merging %d file(s) into %s", clustercount, filename);
        fprintf(stderr, "Opening %s for writing!\n", filename);

        hdfsFile writeFile = hdfsOpenFile(fs, filename, O_CREAT | O_WRONLY, 0, filereplication, 0);

        if (!writeFile)
        {
            fprintf(stderr, "Failed to open %s for writing!\n", filename);
            return EXIT_FAILURE;
        }

        tSize totalBytesWritten = 0;
        for (unsigned node = 0; node < clustercount; node++)
        {
            if (node > 0)
            {
                writeFile = hdfsOpenFile(fs, filename, O_WRONLY | O_APPEND, 0, filereplication, 0);
                fprintf(stderr, "Re-opening %s for append!\n", filename);
            }

            unsigned bytesWrittenSinceLastFlush = 0;

            string filepartname;
            filepartname.append(filename);
            filepartname.append("-parts/part_");
            stringstream ss;
            ss << nodeid;
            filepartname.append(ss.str());
            filepartname.append("_");
            ss.str("");
            ss << clustercount;
            filepartname.append(ss.str());

            if (hdfsExists(fs, filepartname.c_str()) == 0)
            {

                fprintf(stderr, "Opening readfile  %s\n", filepartname.c_str());
                hdfsFile readFile = hdfsOpenFile(fs, filepartname.c_str(), O_RDONLY, 0, 0, 0);
                if (!readFile)
                {
                    fprintf(stderr, "Failed to open %s for reading!\n", filename);
                    return EXIT_FAILURE;
                }

                unsigned char buffer[bufferSize + 1];

                while (hdfsAvailable(fs, readFile))
                {
                    tSize num_read_bytes = hdfsRead(fs, readFile, buffer, bufferSize);

                    if (num_read_bytes <= 0)
                        break;

                    tSize bytesWritten = 0;
                    try
                    {
                        bytesWritten = hdfsWrite(fs, writeFile, (void*) buffer, num_read_bytes);
                        totalBytesWritten += bytesWritten;
                        bytesWrittenSinceLastFlush += bytesWritten;

                        if (bytesWrittenSinceLastFlush >= flushthreshold)
                        {
                            if (hdfsFlush(fs, writeFile))
                            {
                                fprintf(stderr, "Failed to 'flush' %s\n", filename);
                                return EXIT_FAILURE;
                            }
                            bytesWrittenSinceLastFlush = 0;
                        }
                    } catch (...)
                    {
                        fprintf(stderr, "Issue detected during HDFSWrite\n");
                        fprintf(stderr, "Bytes written in current iteration: %d\n", bytesWritten);
                        return EXIT_FAILURE;
                    }
                }

                if (hdfsFlush(fs, writeFile))
                {
                    fprintf(stderr, "Failed to 'flush' %s\n", filename);
                    return EXIT_FAILURE;
                }

                fprintf(stderr, "Closing readfile  %s\n", filepartname.c_str());
                hdfsCloseFile(fs, readFile);

                if (deleteparts)
                {
                    hdfsDelete(fs, filepartname.c_str());
                }
            }
            else
            {
                fprintf(stderr, "Could not merge, part %s was not located\n", filepartname.c_str());
                return EXIT_FAILURE;
            }

            if (deleteparts)
            {
                filepartname.assign(filename);
                filepartname.append("-parts");
                hdfsDelete(fs, filepartname.c_str());
            }

            fprintf(stderr, "Closing writefile %s\n", filename);
            hdfsCloseFile(fs, writeFile);
        }
    }
    return EXIT_SUCCESS;
}

int Hdfs_Connector::writeFlatOffset(const char * filename, unsigned nodeid, unsigned clustercount,
        const char * fileorpipename)
{
    if (!fs)
    {
        fprintf(stderr, "Could not connect to hdfs on");
        return RETURN_FAILURE;
    }

    string filepartname;
    filepartname.append(filename);
    filepartname.append("-parts/part_");
    stringstream ss;
    ss << nodeid;
    filepartname.append(ss.str());
    filepartname.append("_");
    ss.str("");
    ss << clustercount;
    filepartname.append(ss.str());

    hdfsFile writeFile = hdfsOpenFile(fs, filepartname.c_str(), O_CREAT | O_WRONLY, 0, 1, 0);

    if (!writeFile)
    {
        fprintf(stderr, "Failed to open %s for writing!\n", filepartname.c_str());
        return RETURN_FAILURE;
    }

    fprintf(stderr, "Opened HDFS file %s for writing successfully...\n", filepartname.c_str());

    fprintf(stderr, "Opening pipe:  %s \n", fileorpipename);

    ifstream in;
    in.open(fileorpipename, ios::in | ios::binary);

    char char_ptr[124 * 100]; //TODO: this should be configurable.
                                // should it be bigger/smaller?
                                // should it match the HDFS file block size?

    size_t bytesread = 0;
    size_t totalbytesread = 0;
    size_t totalbyteswritten = 0;

    fprintf(stderr, "Writing %s to HDFS.", filepartname.c_str());
    while (!in.eof())
    {
        memset(&char_ptr[0], 0, sizeof(char_ptr));
        in.read(char_ptr, sizeof(char_ptr));
        bytesread = in.gcount();
        totalbytesread += bytesread;
        tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*) char_ptr, bytesread);
        totalbyteswritten += num_written_bytes;

        //Need to figure out how often this should be done
        //if(totalbyteswritten % )
        {
            if (hdfsFlush(fs, writeFile))
            {
                fprintf(stderr, "Failed to 'flush' %s\n", filepartname.c_str());
                return EXIT_FAILURE;
            }
        }
    }
    in.close();

    if (hdfsFlush(fs, writeFile))
    {
        fprintf(stderr, "Failed to 'flush' %s\n", filepartname.c_str());
        return EXIT_FAILURE;
    }

    fprintf(stderr, "\n total read: %lu, total written: %lu\n", totalbytesread, totalbyteswritten);

    int clos = hdfsCloseFile(fs, writeFile);
    fprintf(stderr, "hdfsCloseFile result: %d", clos);

    return EXIT_SUCCESS;
}

void Hdfs_Connector::escapedStringToChars(const char * source, string & escaped)
{
    int si = 0;

    while (source[si])
    {
        if (source[si] == '\\')
        {
            switch (source[++si])
            {
            case 'n':
                escaped.append(1, '\n');
                break;
            case 'r':
                escaped.append(1, '\r');
                break;
            case 't':
                escaped.append(1, '\t');
                break;
            case 'b':
                escaped.append(1, '\b');
                break;
            case 'v':
                escaped.append(1, '\v');
                break;
            case 'f':
                escaped.append(1, '\f');
                break;
            case '\\':
                escaped.append(1, '\\');
                break;
            case '\'':
//fprintf(stderr, "adding escaped single quote..");
                escaped.append(1, '\'');
                break;
            case '\"':
                escaped.append(1, '\"');
                break;
            case '0':
                escaped.append(1, '\0');
                break;
//			case 'c':
//				escaped.append(1,'\c');
//				break;
            case 'a':
                escaped.append(1, '\a');
                break;
//			case 's':
//				escaped.append(1,'\s');
//				break;
            case 'e':
                escaped.append(1, '\e');
                break;
            default:
                break;

            }
        }
        else
            escaped.append(1, source[si]);

        si++;
    }
}

int main(int argc, char **argv)
{
    Hdfs_Connector * connector = NULL;

    unsigned int bufferSize = 1024 * 100;
    unsigned int flushThreshold = bufferSize * 10;
    int returnCode = EXIT_FAILURE;
    unsigned clusterCount = 0;
    unsigned nodeID = 0;
    unsigned long recLen = 0;
    unsigned long maxLen = 0;
    const char * fileName = "";
    const char * hadoopHost = "default";
    int hadoopPort = 0;
    string format("");
    string foptions("");
    string data("");

    int currParam = 1;

    const char * wuid = "";
    const char * rowTag = "Row";
    const char * separator = "";
    string terminator(EOL);
    bool outputTerminator = true;
    string quote("'");
    const char * headerText = "<Dataset>";
    const char * footerText = "</Dataset>";
    const char * hdfsuser = "";
    const char * hdfsgroup = "";
    const char * pipepath = "";
    bool cleanmerge = false;
    short filereplication = 1;

    enum HadoopPluginAction
    {
        HPA_INVALID = -1,
        HPA_STREAMIN = 0,
        HPA_STREAMOUT = 1,
        HPA_STREAMOUTPIPE = 2,
        HPA_READOUT = 3,
        HPA_MERGEFILE = 4
    };

    HadoopPluginAction action = HPA_INVALID;

    while (currParam < argc)
    {
        if (strcmp(argv[currParam], "-si") == 0)
        {
            action = HPA_STREAMIN;
        }
        else if (strcmp(argv[currParam], "-so") == 0)
        {
            action = HPA_STREAMOUT;
        }
        else if (strcmp(argv[currParam], "-sop") == 0)
        {
            action = HPA_STREAMOUTPIPE;
        }
        else if (strcmp(argv[currParam], "-mf") == 0)
        {
            action = HPA_MERGEFILE;
        }
        else if (strcmp(argv[currParam], "-clustercount") == 0)
        {
            clusterCount = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-nodeid") == 0)
        {
            nodeID = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-reclen") == 0)
        {
            recLen = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-format") == 0)
        {
            const char * tmp = argv[++currParam];
            while (*tmp && *tmp != '(')
                format.append(1, *tmp++);
            fprintf(stderr, "Format: %s", format.c_str());
            if (*tmp++)
                while (*tmp && *tmp != ')')
                    foptions.append(1, *tmp++);
        }
        else if (strcmp(argv[currParam], "-rowtag") == 0)
        {
            rowTag = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-filename") == 0)
        {
            fileName = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-host") == 0)
        {
            hadoopHost = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-port") == 0)
        {
            hadoopPort = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-wuid") == 0)
        {
            wuid = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-data") == 0)
        {
            data.append(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-separator") == 0)
        {
            separator = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-terminator") == 0)
        {
            terminator.clear();
            connector->escapedStringToChars(argv[++currParam], terminator);
        }
        else if (strcmp(argv[currParam], "-quote") == 0)
        {
            quote.clear();
            connector->escapedStringToChars(argv[++currParam], quote);
        }
        else if (strcmp(argv[currParam], "-headertext") == 0)
        {
            headerText = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-footertext") == 0)
        {
            footerText = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-buffsize") == 0)
        {
            bufferSize = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-outputterminator") == 0)
        {
            outputTerminator = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-maxlen") == 0)
        {
            maxLen = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-hdfsuser") == 0)
        {
            hdfsuser = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-hdfsgroup") == 0)
        {
            hdfsgroup = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-pipepath") == 0)
        {
            pipepath = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-flushsize") == 0)
        {
            flushThreshold = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-cleanmerge") == 0)
        {
            cleanmerge = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-hdfsfilereplication") == 0)
        {
            filereplication = atoi(argv[++currParam]);
        }
        else
        {
            fprintf(stderr, "Error: Found invalid input param: %s \n", argv[currParam]);
            return returnCode;
        }
        currParam++;
    }

    connector = new Hdfs_Connector(hadoopHost, hadoopPort, hdfsuser);

    hdfsFS fs = connector->getHdfsFS();

    if (fs)
    {
        if (action == HPA_STREAMIN)
        {
            fprintf(stderr, "\nStreaming in %s...\n", fileName);

            unsigned long fileSize = connector->getFileSize(fileName);
            if (fileSize != RETURN_FAILURE)
            {
                if (strcmp(format.c_str(), "FLAT") == 0)
                {
                    unsigned long recstoread = connector->getRecordCount(fileSize, clusterCount, recLen, nodeID);
                    if (recstoread == RETURN_FAILURE)
                    {
                        if (fs)
                            hdfsDisconnect(fs);

                        return EXIT_FAILURE;
                    }

                    unsigned long offset = nodeID * (fileSize / clusterCount / recLen) * recLen;
                    if ((fileSize / recLen) % clusterCount > 0)
                    {
                        if ((fileSize / recLen) % clusterCount > nodeID)
                            offset += nodeID * recLen;
                        else
                            offset += ((fileSize / recLen) % clusterCount) * recLen;
                    }

                    fprintf(stderr, "fileSize: %lu offset: %lu size bytes: %lu, recstoread:%lu\n", fileSize, offset,
                            recstoread * recLen, recstoread);
                    if (offset < fileSize)
                        returnCode = connector->readFileOffset(fileName, offset, recstoread * recLen, bufferSize);
                }
                else if (strcmp(format.c_str(), "CSV") == 0)
                {
                    fprintf(stderr, "Filesize: %ld, Offset: %ld, readlen: %ld\n", fileSize,
                            (fileSize / clusterCount) * nodeID, fileSize / clusterCount);

                    returnCode = connector->readCSVOffset(fileName, (fileSize / clusterCount) * nodeID,
                            fileSize / clusterCount, terminator.c_str(), bufferSize, outputTerminator, recLen, maxLen,
                            quote.c_str());
                }
                else if (strcmp(format.c_str(), "XML") == 0)
                {
                    fprintf(stderr, "Filesize: %ld, Offset: %ld, readlen: %ld\n", fileSize,
                            (fileSize / clusterCount) * nodeID, fileSize / clusterCount);

                    returnCode = connector->readXMLOffset(fileName, (fileSize / clusterCount) * nodeID,
                            fileSize / clusterCount, rowTag, headerText, footerText, bufferSize);
                }
                else
                    fprintf(stderr, "Unknown format type: %s(%s)", format.c_str(), foptions.c_str());
            }
            else
                fprintf(stderr, "Could not determine HDFS file size: %s", fileName);
        }
        else if (action == HPA_STREAMOUT)
        {
            returnCode = connector->writeFlatOffset(fileName, nodeID, clusterCount, pipepath);
        }
        else if (action == HPA_STREAMOUTPIPE)
        {
            returnCode = connector->writeFlatOffset(fileName, nodeID, clusterCount, pipepath);
        }
        else if (action == HPA_MERGEFILE)
        {
            returnCode = connector->mergeFile(fileName, nodeID, clusterCount, bufferSize, flushThreshold,
                    filereplication, cleanmerge);
        }
        else
        {
            fprintf(stderr, "\nNo action type detected, exiting.");
            returnCode = EXIT_FAILURE;
        }

        if (connector)
            delete (connector);
    }
    else
    {
        fprintf(stderr, "Could not connect to hdfs on %s:%d\n", hadoopHost, hadoopPort);
        if(action == HPA_STREAMOUTPIPE)
        {
            fprintf(stderr, "Attempting to close named pipe: %s\n", pipepath);
            ifstream in;
            in.open(pipepath, ios::in | ios::binary);
            in.close();
        }
    }
    return returnCode;
}
