/*##############################################################################
#    Copyright (C) 2014 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

/* HDFSConnector
Pipe data to and from Hadoop

It is necessary to add this option to your workunit:
#option('pickBestEngine', 0);

This will force your HDFSConnector job to run on the target cluster (as opposed to the optimizer
picking hThor when you've selected Thor, for instance) so that the data lands where you want
it.

For PipeOutAndMerge to work the target HDFS must have append support enabled.  Be default it's disabled.  To
enable it add this to hdfs-site.xml:
        <property>
            <name>dfs.support.append</name>
            <value>true</value>
            <final>true</value>
        </property>
*/

import std;

EXPORT HDFSConnector := MODULE

    /*
     HDFSConnector.PipeIn - this macro to be called by the user to pipe in data from the Hadoop file system (HDFS).

     @param ECL_RS            The ECL recordset to pipe into.
     @param HadoopFileName    The fully qualified target HDFS file name.
     @param Layout            The structure which describes the ECL_RS recordset.
     @param HadoopFileFormat  The Hadoop data file format : FLAT | CSV.
     @param HDFSHost          The Hadoop DFS host name or IP address.
     @param HDSFPort          The Hadoop DFS port number.
                              If targeting a local HDFS HDFSHost='default' and HDSFPort=0 will work
                              As long as the local hadoop conf folder is visible to the 'hdfspipe' script
    */

    export PipeIn(ECL_RS, HadoopFileName, Layout, HadoopFileFormat, HDFSHost, HDSFPort, HDFSUser='', ConnectorOptions='') := MACRO
  #uniquename(mywuid)
  %mywuid% := ' -wuid ' + STD.system.Job.wuid();
    #uniquename(formatstr)
        %formatstr% := STD.Str.FilterOut(#TEXT(HadoopFileFormat), ' \t\n\r');
        #IF(%formatstr%[1..3] = 'XML')
            #IF (LENGTH(%formatstr%) > 3)
                #uniquename(rowtagcont)
                #uniquename(firsttok)
                %firsttok% := STD.Str.Extract(%formatstr%[4..],1);
                %rowtagcont% := %firsttok%[STD.Str.Find(%firsttok%, '\'',1)+1..STD.Str.Find(%firsttok%, '\'',2)-1];

                #uniquename(headingpos)
                %headingpos% := STD.Str.Find(%formatstr%, 'HEADING');
                #IF (%headingpos% > 0)
                    #uniquename(headingcont)
                    #uniquename(headingcont2)
                    #uniquename(headertext)
                    #uniquename(footertext)
                    %headingcont% := %formatstr%[%headingpos%+SIZEOF('HEADING')..];
                    %headingcont2%:= %headingcont%[STD.Str.Find(%headingcont%, '(')+1..STD.Str.Find(%headingcont%, ')')-1];

                    %headertext% := STD.Str.Extract(%headingcont2%,1);
                    %footertext% := STD.Str.Extract(%headingcont2%,2);
                #END
            #ELSE
                %rowtagcont% := 'Row';
            #END
            ECL_RS:= PIPE('hdfspipe -si '
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                + ' -filename ' + HadoopFileName
                + ' -format ' +  %formatstr%[1..3]
                + ' -rowtag ' + %rowtagcont%
                // + ' -headertext ' + '???'
                // + ' -footertext ' + '???'
                + ' -host ' + HDFSHost + ' -port ' + HDSFPort
                #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
                #END

                #IF ( LENGTH(#TEXT(HDFSUser)) > 0)
                + ' -hdfsuser ' + HDFSUser
                #END
                + %mywuid%,
                Layout, HadoopFileFormat);

        #ELSEIF (%formatstr%[1..3] = 'CSV')
            #uniquename(quoteseq)
            %quoteseq% := REGEXFIND('(.*)(?i)(QUOTE)(\\s*\\(\\s*)(\'.*?\')\\s*\\)\\s*,?', %formatstr%,4);

            #uniquename(terminatorseq)
            %terminatorseq% := REGEXFIND('(.*)(?i)(TERMINATOR)(\\s*\\(\\s*)(\'.*?\')\\s*\\)\\s*,?', %formatstr%,4);

            #uniquename(pipecmndstr)
            %pipecmndstr% := 'hdfspipe -si '
              + ' -host ' + HDFSHost    + ' -port ' + HDSFPort
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                + ' -maxlen ' + sizeof(Layout, MAX)
                + ' -filename ' + HadoopFileName
                + ' -format CSV'
                + %mywuid%
                + ' -whdfsretrymax 10 '
            #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
            #END

            #IF ( LENGTH(%terminatorseq%) > 0)
                + ' -terminator ' + %terminatorseq%
            #END

            #IF ( LENGTH(%quoteseq%) > 0)
                + ' -quote ' + '\'' + %quoteseq% + '\''
            #END

            #IF ( LENGTH(#TEXT(HDFSUser)) > 0)
                + ' -hdfsuser ' + HDFSUser
            #END
                ; //Do not remove terminating semicolon

            ECL_RS:= PIPE( %pipecmndstr%, Layout, HadoopFileFormat);
        #ELSE
                ECL_RS:= PIPE('hdfspipe -si'
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                + ' -reclen ' + sizeof(Layout)
                + ' -filename ' + HadoopFileName
                + ' -format '   +  %formatstr%
                + ' -host ' + HDFSHost + ' -port ' + HDSFPort
                #IF ( LENGTH(#TEXT(HDFSUser)) > 0)
                + ' -hdfsuser ' + HDFSUser
                #END

                #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
                #END
                + %mywuid%,
                Layout);
        #END
    ENDMACRO;

    /*
    HDFSConnector.PipeOut - writes the given recordset 'ECL_RS' to the target HDFS system in
                            file parts. One file part for each HPCC node.

    ECL_RS              - The ECL recordset to pipe out.
    HadoopFileName      - The fully qualified target HDFS file name.
    Layout              - The structure which describes the ECL_RS recordset.
    HadoopFileFormat    - The Hadoop data file format : FLAT | CSV
    HDFSHost            - The Hadoop DFS host name or IP address.
    HDSFPort            - The Hadoop DFS port number.
    HDFSUser            - HDFS username to use to login to HDFS in order to write the file
                          must have permission to write to the target HDFS location.

    Example:

    HDFSConnector.PipeOut(sue, '/user/hadoop/HDFSAccounts', Layout_CSV_Accounts, CSV, '192.168.56.102', '54310', 'hadoop');
    HDFSConnector.PipeOut(sue, '/user/hadoop/HDFSPersons', Layout_Flat_Persons, FLAT, '192.168.56.102', '54310', 'hadoop');
    */

    export PipeOut(ECL_RS, HadoopFileName, Layout, HadoopFileFormat, HDFSHost, HDSFPort, HDFSUser) := MACRO
    #uniquename(mywuid)
    %mywuid% := ' -wuid ' + STD.system.Job.wuid();
    #uniquename(formatstr)
    #uniquename(outpartaction)
    #uniquename(mergepartsaction)
        %formatstr% := STD.Str.FilterOut(#TEXT(HadoopFileFormat), ' \t\n\r');
        #IF(%formatstr%[1..4] != 'FLAT')
        OUTPUT(ECL_RS,,
                PIPE('hdfspipe -sop '
                + ' -host ' + HDFSHost
                + ' -port ' + HDSFPort
                + ' -filename ' + HadoopFileName
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                + ' -hdfsuser ' + HDFSUser
                #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
                #END
                + %mywuid%, HadoopFileFormat));
        #ELSE
        OUTPUT(ECL_RS,,
                PIPE('hdfspipe -sop '
                + ' -host ' + HDFSHost
                + ' -port ' + HDSFPort
                + ' -filename ' + HadoopFileName
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
                #END
                + ' -hdfsuser ' + HDFSUser
                + %mywuid%));
        #END
    ENDMACRO;

    /*
    HDFSConnector.PipeOutAndMerge - writes the given recordset 'ECL_RS' to the target HDFS system
                                    in file parts and merges them together to form a single target file
                                    on the HDFS system.

    ECL_RS          - The ECL recordset to pipe out.
    HadoopFileName  - The fully qualified target HDFS file name.
    Layout          - The structure which describes the ECL_RS recordset
    HadoopFileFormat- The Hadoop data file format : FLAT | CSV
    HDFSHost        - The Hadoop DFS host name or IP address.
    Port            - The Hadoop DFS port number.
    HDFSUser        - HDFS username to use to login to HDFS in order to write the file
                      must have permission to write to the target HDFS location.

    Example:

    HDFSConnector.PipeOut(sue, '/user/hadoop/HDFSAccounts', Layout_CSV_Accounts, CSV, '192.168.56.102', '54310', 'hadoop');
    HDFSConnector.PipeOut(sue, '/user/hadoop/HDFSPersons', Layout_Flat_Persons, FLAT, '192.168.56.102', '54310', 'hadoop');
    */

    export PipeOutAndMerge(ECL_RS, HadoopFileName, Layout, HadoopFileFormat, HDFSHost, HDSFPort, HDFSUser) := MACRO
    #uniquename(mywuid)
    %mywuid% := ' -wuid ' + STD.system.Job.wuid();
    #uniquename(formatstr)
    #uniquename(outpartaction)
    #uniquename(mergepartsaction)
        %formatstr% := STD.Str.FilterOut(#TEXT(HadoopFileFormat), ' \t\n\r');
        #IF(%formatstr%[1..4] != 'FLAT')
        //%mergepartsaction% :=DISTRIBUTE(ECL_RS , 1);
        %outpartaction%:=OUTPUT(ECL_RS,,
                PIPE('hdfspipe -sop '
                + ' -host ' + HDFSHost
                + ' -port ' + HDSFPort
                + ' -filename ' + HadoopFileName
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                + ' -hdfsuser ' + HDFSUser
                #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
                #END
                + %mywuid%, HadoopFileFormat));

                %mergepartsaction%:=OUTPUT(PIPE('hdfspipe -mf'
                 + ' -nodeid ' + STD.system.Thorlib.node()
                 + ' -clustercount ' + STD.system.Thorlib.nodes()
                 + ' -filename ' + HadoopFileName
                 + ' -cleanmerge  1'
                 + ' -hdfsuser ' + HDFSUser
                 + ' -host ' + HDFSHost     + ' -port ' + HDSFPort
                 #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                 + ' ' + ConnectorOptions
                 #END
                 + %mywuid%, Layout));
                 SEQUENTIAL(%outpartaction%, %mergepartsaction%);
        #ELSE
        %outpartaction%:=OUTPUT(ECL_RS,,
                PIPE('hdfspipe -sop '
                + ' -host ' + HDFSHost
                + ' -port ' + HDSFPort
                + ' -filename ' + HadoopFileName
                + ' -nodeid ' + STD.system.Thorlib.node()
                + ' -clustercount ' + STD.system.Thorlib.nodes()
                + ' -hdfsuser ' + HDFSUser
                #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                + ' ' + ConnectorOptions
                #END
                + %mywuid%));

                %mergepartsaction%:=OUTPUT(PIPE('hdfspipe -mf'
                 + ' -nodeid ' + STD.system.Thorlib.node()
                 + ' -clustercount ' + STD.system.Thorlib.nodes()
                 + ' -filename ' + HadoopFileName
                 + ' -cleanmerge  1'
                 + ' -hdfsuser ' + HDFSUser
                 + ' -host ' + HDFSHost + ' -port ' + HDSFPort
                 #IF ( LENGTH(#TEXT(ConnectorOptions)) > 0)
                 + ' ' + ConnectorOptions
                 #END
                 + %mywuid%, Layout));
                 SEQUENTIAL(%outpartaction%, %mergepartsaction%);
        #END
    ENDMACRO;

END;
