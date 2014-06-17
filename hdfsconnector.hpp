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
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <stdexcept>

using namespace std;

#define EOL "\n"
#define RETURN_FAILURE -1

enum HDFSConnectorAction
{
    HCA_INVALID = -1,
    HCA_STREAMIN = 0,
    HCA_STREAMOUT = 1,
    HCA_STREAMOUTPIPE = 2,
    HCA_READOUT = 3,
    HCA_MERGEFILE = 4
};

struct HdfsFileStatus
{
   long accessTime;
   int blockSize;
   const char * group;
   long length;
   long modificationTime;
   const char * owner;
   const char * pathSuffix;
   const char * permission;
   int replication;
   const char * type; //"FILE" | "DIRECTORY"
};

template <class T>
inline string template2string (const T& anything)
{
    stringstream ss;
    ss << anything;
    return ss.str();
}

static inline void createFilePartName(string * filepartname, const char * filename, unsigned int nodeid, unsigned int clustercount)
{
    filepartname->append(filename);
    filepartname->append("-parts/part_");
    filepartname->append(template2string(nodeid));
    filepartname->append("_");
    filepartname->append(template2string(clustercount));
}

static void expandEscapedChars(const char * source, string & escaped)
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
                escaped.append(1, '\'');
                break;
            case '\"':
                escaped.append(1, '\"');
                break;
            case '0':
                escaped.append(1, '\0');
                break;
            case 'a':
                escaped.append(1, '\a');
                break;
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
class hdfsconnector
{
protected:
    HDFSConnectorAction action;
    unsigned int bufferSize;
    unsigned int flushThreshold;
    unsigned clusterCount;
    unsigned nodeID;
    unsigned long recLen;
    unsigned long maxLen;
    const char * fileName;
    const char * hadoopHost;
    int hadoopPort;
    string format;
    string foptions;
    string data;
    const char * wuid;
    const char * rowTag;
    const char * separator;
    string terminator;
    bool outputTerminator;
    string quote;
    const char * headerText;
    const char * footerText;
    const char * hdfsuser;
    const char * hdfsgroup;
    const char * pipepath;
    bool cleanmerge;
    short filereplication;
    unsigned short maxRetry;
    int blockSize;
    bool verbose;
public:
    hdfsconnector() {};

    virtual ~hdfsconnector(){};

    virtual bool connect() = 0;
    virtual int  execute() = 0 ;
    virtual int streamFileOffset() = 0;
    virtual int writeFlatOffset() = 0;
    virtual int mergeFile() = 0;

    virtual bool validateParameters()
    {
        bool validated = true;

        if (clusterCount <= 0)
        {
            fprintf(stderr, "\nInvalid cluster count detected: %d\n", clusterCount);
            validated = false;
        }

        //nodeID is expected to be a 0-indexed value, as reported by thorlib.node()
        if (nodeID >= clusterCount)
        {
            fprintf(stderr, "\nInvalid nodeID detected: %d\n", nodeID);
            validated = false;
        }

        return validated;
    }

    unsigned static getUnsignedIntFromStr(const char * strrepresentation)
    {
        int tmp = atoi(strrepresentation);
        if (tmp >= 0)
            return (unsigned)tmp;
        else
        {
            fprintf(stderr, "Unexpected value detected while converting string to unsigned value: %s.\n", strrepresentation);
            throw;
        }
    }

    bool parseInParams (int argc, char **argv)
    {
        int returnCode = EXIT_FAILURE;
        int currParam = 1;

        bufferSize = 1024 * 100;
        flushThreshold = bufferSize * 10;
        clusterCount = 0;
        nodeID = 0;
        recLen = 0;
        maxLen = 0;
        fileName = "";
        hadoopHost = "default";
        hadoopPort = 0;
        format = "";
        foptions = "";
        data = "";

        wuid = "";
        rowTag = "Row";
        separator = "";
        terminator = EOL;
        outputTerminator = true;
        quote = "'";
        headerText = "<Dataset>";
        footerText = "</Dataset>";
        hdfsuser = "";
        hdfsgroup = "";
        pipepath = "";
        cleanmerge = false;
        filereplication = 1;
        maxRetry = 1;
        blockSize = 0;
        verbose = false;

        action = HCA_INVALID;

        bool allvalid = true;

        try
        {
            while (currParam < argc)
            {
                if (strcmp(argv[currParam], "-si") == 0)
                {
                    action = HCA_STREAMIN;
                    fprintf(stderr, "Action: HCA_STREAMIN\n");
                }
                else if (strcmp(argv[currParam], "-so") == 0)
                {
                    action = HCA_STREAMOUT;
                    fprintf(stderr, "Action: HCA_STREAMOUT\n");
                }
                else if (strcmp(argv[currParam], "-sop") == 0)
                {
                    action = HCA_STREAMOUTPIPE;
                    fprintf(stderr, "Action: HCA_STREAMOUTPIPE\n");
                }
                else if (strcmp(argv[currParam], "-mf") == 0)
                {
                    action = HCA_MERGEFILE;
                    fprintf(stderr, "Action: HCA_MERGEFILE\n");
                }
                else if (strcmp(argv[currParam], "-clustercount") == 0)
                {
                    fprintf(stderr, "clustercount: ");
                    try
                    {
                        clusterCount = getUnsignedIntFromStr(argv[++currParam]);
                        fprintf(stderr, "%d\n", clusterCount);
                    }
                    catch(...)
                    {
                        allvalid = false;
                    }
                }
                else if (strcmp(argv[currParam], "-nodeid") == 0)
                {
                    fprintf(stderr, "nodeID: ");
                    try
                    {
                        nodeID = getUnsignedIntFromStr(argv[++currParam]);
                        fprintf(stderr, "%d\n", nodeID);
                    }
                    catch (...)
                    {
                        allvalid = false;
                    }
                }
                else if (strcmp(argv[currParam], "-reclen") == 0)
                {
                    recLen = atol(argv[++currParam]);
                    fprintf(stderr, "recLen: %ld\n", recLen);
                }
                else if (strcmp(argv[currParam], "-format") == 0)
                {
                    const char * tmp = argv[++currParam];
                    while (tmp && *tmp && *tmp != '(')
                        format.append(1, *tmp++);
                    fprintf(stderr, "Format: %s\n", format.c_str());
                    if (tmp && *tmp && *tmp=='(')
                    {
                        tmp++;
                        while (tmp && *tmp && *tmp != ')')
                            foptions.append(1, *tmp++);
                    }
                }
                else if (strcmp(argv[currParam], "-rowtag") == 0)
                {
                    rowTag = argv[++currParam];
                }
                else if (strcmp(argv[currParam], "-filename") == 0)
                {
                    fileName = argv[++currParam];
                    fprintf(stderr, "fileName: %s\n", fileName);
                }
                else if (strcmp(argv[currParam], "-host") == 0)
                {
                    hadoopHost = argv[++currParam];
                    fprintf(stderr, "host: %s\n", hadoopHost);
                }
                else if (strcmp(argv[currParam], "-port") == 0)
                {
                    hadoopPort = atoi(argv[++currParam]);
                    fprintf(stderr, "port: %d\n", hadoopPort);
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
                    expandEscapedChars(argv[++currParam], terminator);
                }
                else if (strcmp(argv[currParam], "-quote") == 0)
                {
                    quote.clear();
                    expandEscapedChars(argv[++currParam], quote);
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
                    fprintf(stderr, "hdfsuser: %s\n", hdfsuser);
                }
                else if (strcmp(argv[currParam], "-hdfsgroup") == 0)
                {
                    hdfsgroup = argv[++currParam];
                    fprintf(stderr, "hdfsgroup: %s\n", hdfsgroup);
                }
                else if (strcmp(argv[currParam], "-pipepath") == 0)
                {
                    pipepath = argv[++currParam];
                    fprintf(stderr, "pipepath: %s\n", pipepath);
                }
                else if (strcmp(argv[currParam], "-flushsize") == 0)
                {
                    flushThreshold = atol(argv[++currParam]);
                    fprintf(stderr, "flushThreshold: %d\n", flushThreshold);
                }
                else if (strcmp(argv[currParam], "-cleanmerge") == 0)
                {
                    cleanmerge = atoi(argv[++currParam]);
                    fprintf(stderr, "cleanmerge: %d\n", cleanmerge);
                }
                else if (strcmp(argv[currParam], "-hdfsfilereplication") == 0)
                {
                    filereplication = atoi(argv[++currParam]);
                    fprintf(stderr, "hdfsfilereplication: %d\n", filereplication);
                }
                else if (strcmp(argv[currParam], "-whdfsretrymax") == 0)
                {
                    maxRetry = atoi(argv[++currParam]);
                }
                else if (strcmp(argv[currParam], "-verbose") == 0)
                {
                   verbose = atoi(argv[++currParam]);
                }
                else if (strcmp(argv[currParam], "-blocksize") == 0)
                {
                    blockSize = atoi(argv[++currParam]);
                }
                else
                {
                    fprintf(stderr, "Error: Found invalid input param: %s \n", argv[currParam]);
                    allvalid = false;
                }
                currParam++;
            }
        }
        catch(...)
        {
            fprintf(stderr, "Error: found unexpected value while parsing input parameters\n");
            return false;
        }
        return allvalid;
    }
};

