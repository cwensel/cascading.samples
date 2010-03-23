/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class RegexParserMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
  {
  private Pattern pattern;
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
