/*
 * Copyright (c) 2008, Vinculum Technologies, Inc. All Rights Reserved.
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
    Tap remoteLogTap = new Hfs( new TextLine( apacheFields ), outputPath );

    // connect the assembly to the SOURCE and SINK taps
    Flow parsedLogFlow = new FlowConnector().connect( localLogTap, remoteLogTap, importPipe );

    // optionally print out the parsedLogFlow to a graph file for import into a graphics package
    // this is useful for visualizing the flow to help with debugging
    //parsedLogFlow.writeDOT( "logparser.dot" );

    // start execution of the flow (either locally or on the cluster
    parsedLogFlow.start();

    // block until the flow completes
    parsedLogFlow.complete();
    }
  }
