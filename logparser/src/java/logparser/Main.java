/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package logparser;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class Main
  {
  public static void main( String[] args )
    {
    String inputPath = args[ 0 ];
    String outputPath = args[ 1 ];

    // create SOURCE tap to read a resource from the local file system
    // be default the TextLine scheme declares two fields, "offset" and "line"
    Tap localLogTap = new Lfs( new TextLine(), inputPath );

    // create an assembly to parse an Apache log file and store on an HDFS cluster

    // declare the field names we will parse out of the log file
    Fields apacheFields = new Fields( "ip", "time", "method", "event", "status", "size" );

    // define the regular expression to parse the log file with
    String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

    // declare the groups from the above regex we want to keep. each regex group will be given
    // a field name from 'apacheFields', above, respectively
    int[] allGroups = {1, 2, 3, 4, 5, 6};

    // create the stream parser
    RegexParser parser = new RegexParser( apacheFields, apacheRegex, allGroups );

    // create the parser pipe element, with the name 'parser', and with the input field name 'line'
    Pipe importPipe = new Each( "parser", new Fields( "line" ), parser );

    // create a SINK tap to write to the default filesystem
    // by default, TextLine writes all fields out
    Tap remoteLogTap = new Hfs( new TextLine(), outputPath );

    // connect the assembly to the SOURCE and SINK taps
    Flow parsedLogFlow = new FlowConnector().connect( localLogTap, remoteLogTap, importPipe );

    // optionally print out the parsedLogFlow to a graph file for import into a graphics package
    // this is useful for visualizing the flow to help with debugging
//    parsedLogFlow.writeDOT( "logparser.dot" );

    // start execution of the flow (either locally or on the cluster
    parsedLogFlow.start();

    // block until the flow completes
    parsedLogFlow.complete();
    }
  }
