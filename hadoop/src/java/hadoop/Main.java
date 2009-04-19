/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Main
  {
  public static class RegexParserMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
    Pattern pattern;
    private Matcher matcher;

    @Override
    public void configure( JobConf job )
      {
      pattern = Pattern.compile( job.get( "logparser.regex" ) );
      matcher = pattern.matcher( "" ); // lets re-use the matcher
      }

    @Override
    public void map( LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException
      {
      matcher.reset( value.toString() );

      if( !matcher.find() )
        throw new RuntimeException( "could not match pattern: [" + pattern + "] with value: [" + value + "]" );

      StringBuffer buffer = new StringBuffer();

      for( int i = 0; i < matcher.groupCount(); i++ )
        {
        if( i != 0 )
          buffer.append( "\t" );

        buffer.append( matcher.group( i + 1 ) ); // skip group 0
        }

      // pass null so a TAB is not prepended, not all OutputFormats accept null
      output.collect( null, new Text( buffer.toString() ) );
      }
    }

  public static void main( String[] args ) throws IOException
    {
    // create Hadoop path instances
    Path inputPath = new Path( args[ 0 ] );
    Path outputPath = new Path( args[ 1 ] );

    // get the FileSystem instances for the input path
    FileSystem outputFS = outputPath.getFileSystem( new JobConf() );

    // if output path exists, delete recursively
    if( outputFS.exists( outputPath ) )
      outputFS.delete( outputPath, true );

    // initialize Hadoop job configuration
    JobConf jobConf = new JobConf();
    jobConf.setJobName( "logparser" );

    // set the current job jar
    jobConf.setJarByClass( Main.class );

    // set the input path and input format
    TextInputFormat.setInputPaths( jobConf, inputPath );
    jobConf.setInputFormat( TextInputFormat.class );

    // set the output path and output format
    TextOutputFormat.setOutputPath( jobConf, outputPath );
    jobConf.setOutputFormat( TextOutputFormat.class );
    jobConf.setOutputKeyClass( Text.class );
    jobConf.setOutputValueClass( Text.class );

    // must set to zero since we have no reducers
    jobConf.setNumReduceTasks( 0 );

    // configure our parsing map classs
    jobConf.setMapperClass( RegexParserMap.class );
    String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";
    jobConf.set( "logparser.regex", apacheRegex );

    // create Hadoop client, must pass in this JobConf for some reason
    JobClient jobClient = new JobClient( jobConf );

    // submit job
    RunningJob runningJob = jobClient.submitJob( jobConf );

    // block until job completes
    runningJob.waitForCompletion();
    }
  }