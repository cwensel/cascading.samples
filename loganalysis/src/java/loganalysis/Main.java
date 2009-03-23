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

package loganalysis;

import java.util.Map;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
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
    // set the current job jar
    Properties properties = new Properties();
    FlowConnector.setApplicationJarClass( properties, Main.class );

    FlowConnector flowConnector = new FlowConnector( properties );
    CascadeConnector cascadeConnector = new CascadeConnector();

    String inputPath = args[ 0 ];
    String logsPath = args[ 1 ] + "/logs/";
    String arrivalRatePath = args[ 1 ] + "/arrivalrate/";
    String arrivalRateSecPath = arrivalRatePath + "sec";
    String arrivalRateMinPath = arrivalRatePath + "min";

    // create an assembly to import an Apache log file and store on DFS
    // declares: "time", "method", "event", "status", "size"
    Fields apacheFields = new Fields( "ip", "time", "method", "event", "status", "size" );
    String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";
    int[] apacheGroups = {1, 2, 3, 4, 5, 6};
    RegexParser parser = new RegexParser( apacheFields, apacheRegex, apacheGroups );
    Pipe importPipe = new Each( "import", new Fields( "line" ), parser );

    // create tap to read a resource from the local file system, if not an url for an external resource
    // Lfs allows for relative paths
    Tap logTap =
      inputPath.matches( "^[^:]+://.*" ) ? new Hfs( new TextLine(), inputPath ) : new Lfs( new TextLine(), inputPath );
    // create a tap to read/write from the default filesystem
    Tap parsedLogTap = new Hfs( apacheFields, logsPath );

    // connect the assembly to source and sink taps
    Flow importLogFlow = flowConnector.connect( logTap, parsedLogTap, importPipe );

    // create an assembly to parse out the time field into a timestamp
    // then count the number of requests per second and per minute

    // apply a text parser to create a timestamp with 'second' granularity
    // declares field "ts"
    DateParser dateParser = new DateParser( new Fields( "ts" ), "dd/MMM/yyyy:HH:mm:ss Z" );
    Pipe tsPipe = new Each( "arrival rate", new Fields( "time" ), dateParser, Fields.RESULTS );

    // name the per second assembly and split on tsPipe
    Pipe tsCountPipe = new Pipe( "tsCount", tsPipe );
    tsCountPipe = new GroupBy( tsCountPipe, new Fields( "ts" ) );
    tsCountPipe = new Every( tsCountPipe, Fields.GROUP, new Count() );

    // apply expression to create a timestamp with 'minute' granularity
    // declares field "tm"
    Pipe tmPipe = new Each( tsPipe, new ExpressionFunction( new Fields( "tm" ), "ts - (ts % (60 * 1000))", long.class ) );

    // name the per minute assembly and split on tmPipe
    Pipe tmCountPipe = new Pipe( "tmCount", tmPipe );
    tmCountPipe = new GroupBy( tmCountPipe, new Fields( "tm" ) );
    tmCountPipe = new Every( tmCountPipe, Fields.GROUP, new Count() );

    // create taps to write the results the default filesystem, using the given fields
    Tap tsSinkTap = new Hfs( new TextLine(), arrivalRateSecPath );
    Tap tmSinkTap = new Hfs( new TextLine(), arrivalRateMinPath );

    // a convenience method for binding taps and pipes, order is significant
    Map<String, Tap> sinks = Cascades.tapsMap( Pipe.pipes( tsCountPipe, tmCountPipe ), Tap.taps( tsSinkTap, tmSinkTap ) );

    // connect the assembly to the source and sink taps
    Flow arrivalRateFlow = flowConnector.connect( parsedLogTap, sinks, tsCountPipe, tmCountPipe );

    // optionally print out the arrivalRateFlow to a graph file for import into a graphics package
    //arrivalRateFlow.writeDOT( "arrivalrate.dot" );

    // connect the flows by their dependencies, order is not significant
    Cascade cascade = cascadeConnector.connect( importLogFlow, arrivalRateFlow );

    // execute the cascade, which in turn executes each flow in dependency order
    cascade.complete();
    }
  }
