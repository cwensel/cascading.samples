/*
 * Copyright (c) 2008, Vinculum Technologies, Inc. All Rights Reserved.
 */

package wordcount;

import java.util.Map;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexReplace;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.xml.TagSoupParser;
import cascading.operation.xml.XPathGenerator;
import cascading.operation.xml.XPathOperation;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.PipeAssembly;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Dfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class Main
  {

  private static class ImportCrawlDataAssembly extends PipeAssembly
    {
    public ImportCrawlDataAssembly( String name )
      {
      // split the text line into "url" and "raw" with the default delimiter of tab
      RegexSplitter regexSplitter = new RegexSplitter( new Fields( "url", "raw" ) );
      Pipe importPipe = new Each( name, new Fields( "line" ), regexSplitter );
      // remove all pdf documents from the stream
      importPipe = new Each( importPipe, new Fields( "url" ), new RegexFilter( ".*\\.pdf$", true ) );
      // replace ":nl" with a new line, return the fields "url" and "page" to the stream.
      // discared the other fields in the stream
      RegexReplace regexReplace = new RegexReplace( new Fields( "page" ), ":nl:", "\n" );
      importPipe = new Each( importPipe, new Fields( "raw" ), regexReplace, new Fields( "url", "page" ) );

      setTails( importPipe );
      }
    }

  private static class WordCountSplitAssembly extends PipeAssembly
    {
    public WordCountSplitAssembly( String sourceName, String sinkUrlName, String sinkWordName )
      {
      // create a new pipe assembly to create the word count across all the pages, and the word count in a single page
      Pipe pipe = new Pipe( sourceName );

      // convert the html to xhtml using the TagSouParser. return only the fields "url" and "xml", discard the rest
      pipe = new Each( pipe, new Fields( "page" ), new TagSoupParser( new Fields( "xml" ) ), new Fields( "url", "xml" ) );
      // apply the given XPath expression to the xml in the "xml" field. this expression extracts the 'body' element.
      XPathGenerator bodyExtractor = new XPathGenerator( new Fields( "body" ), XPathOperation.NAMESPACE_XHTML, "//xhtml:body" );
      pipe = new Each( pipe, new Fields( "xml" ), bodyExtractor, new Fields( "url", "body" ) );
      // apply another XPath expression. this expression removes all elements from the xml, leaving only text nodes.
      // text nodes in a 'script' element are removed.
      String elementXPath = "//text()[ name(parent::node()) != 'script']";
      XPathGenerator elementRemover = new XPathGenerator( new Fields( "words" ), XPathOperation.NAMESPACE_XHTML, elementXPath );
      pipe = new Each( pipe, new Fields( "body" ), elementRemover, new Fields( "url", "words" ) );
      // apply the regex to break the document into individual words and stuff each word at a new tuple into the current
      // stream with field names "url" and "word"
      RegexGenerator wordGenerator = new RegexGenerator( new Fields( "word" ), "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)" );
      pipe = new Each( pipe, new Fields( "words" ), wordGenerator, new Fields( "url", "word" ) );

      // group on "url"
      Pipe urlCountPipe = new GroupBy( sinkUrlName, pipe, new Fields( "url", "word" ) );
      urlCountPipe = new Every( urlCountPipe, new Fields( "url", "word" ), new Count(), new Fields( "url", "word", "count" ) );

      // group on "word"
      Pipe wordCountPipe = new GroupBy( sinkWordName, pipe, new Fields( "word" ) );
      wordCountPipe = new Every( wordCountPipe, new Fields( "word" ), new Count(), new Fields( "word", "count" ) );

      setTails( urlCountPipe, wordCountPipe );
      }
    }

  public static void main( String[] args )
    {
    String inputPath = args[ 0 ];
    String pagesPath = args[ 1 ] + "/pages/";
    String urlsPath = args[ 1 ] + "/urls/";
    String wordsPath = args[ 1 ] + "/words/";
    String localUrlsPath = args[ 2 ] + "/urls/";
    String localWordsPath = args[ 2 ] + "/words/";

    // import a text file with crawled pages from the local filesystem into a Hadoop distributed filesystem
    // the imported file will be a native Hadoop sequence file with the fields "page" and "url"
    // note this examples stores crawl pages as a tabbed file, with the first field being the "url"
    // and the second being the "raw" document that had all new line chars ("\n") converted to the text ":nl:".

    // a predefined pipe assembly that returns fields named "url" and "page"
    Pipe importPipe = new ImportCrawlDataAssembly( "import pipe" );

    // create the tap instances
    Tap localPagesSource = new Lfs( new TextLine(), inputPath );
    Tap importedPages = new Dfs( new SequenceFile( new Fields( "url", "page" ) ), pagesPath );

    // connect the pipe assembly to the tap instances
    Flow importPagesFlow = new FlowConnector().connect( localPagesSource, importedPages, importPipe );

    // a predefined pipe assembly that splits the stream into two named "url pipe" and "word pipe"
    // these pipes could be retrieved via the getTails() method and added to new pipe instances
    PipeAssembly wordCountPipe = new WordCountSplitAssembly( "wordcount pipe", "url pipe", "word pipe" );

    // create Hadoop sequence files to store the results of the counts
    Tap sinkUrl = new Dfs( new SequenceFile( new Fields( "url", "word", "count" ) ), urlsPath );
    Tap sinkWord = new Dfs( new SequenceFile( new Fields( "word", "count" ) ), wordsPath );

    // convenience method to bind multiple pipes and taps
    Map<String, Tap> sinks = Cascades.tapsMap( new String[]{"url pipe", "word pipe"}, Tap.taps( sinkUrl, sinkWord ) );

    // wordCountPipe will be recognized as an assembly and handled appropriately
    Flow count = new FlowConnector().connect( importedPages, sinks, wordCountPipe );

    // create an assembly to export the Hadoop sequence file to local text files
    Pipe exportPipe = new Each( "export pipe", new Identity() );

    Tap localSinkUrl = new Lfs( new TextLine(), localUrlsPath );
    Tap localSinkWord = new Lfs( new TextLine(), localWordsPath );

    // connect up both sinks using the same exportPipe assembly
    Flow exportFromUrlTap = new FlowConnector().connect( sinkUrl, localSinkUrl, exportPipe );
    Flow exportFromWord = new FlowConnector().connect( sinkWord, localSinkWord, exportPipe );

    // connect up all the flows, order is not significant
    Cascade cascade = new CascadeConnector().connect( importPagesFlow, count, exportFromUrlTap, exportFromWord );

    // run the cascade to completion
    cascade.complete();
    }
  }
