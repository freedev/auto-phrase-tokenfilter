package com.lucidworks.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.ExtendedDismaxQParserPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


public class AutoPhrasingQParserPlugin extends ExtendedDismaxQParserPlugin implements ResourceLoaderAware {
	
  private static final Logger Log = LoggerFactory.getLogger( AutoPhrasingQParserPlugin.class );
  private CharArraySet phraseSets;
  private String phraseSetFiles;
  
  private String parserImpl = "lucene";
  
  private char replaceWhitespaceWith = 0;  // preserves stemming
  
  private boolean ignoreCase = true;
	
  @Override
  public void init( NamedList initArgs ) {
    Log.info( "init ..." );
    SolrParams params = SolrParams.toSolrParams(initArgs);
    phraseSetFiles = params.get( "phrases" );

    Log.info( "phraseSetFiles =" + phraseSetFiles );

    String pImpl = params.get( "defType" );

    Log.info( "defType =" + pImpl );

    if (pImpl != null) {
      parserImpl = pImpl;
    }
    
    String replaceWith = params.get( "replaceWhitespaceWith" );
    if (replaceWith != null && replaceWith.length() > 0) {
      replaceWhitespaceWith = replaceWith.charAt(0);
    }

    Log.info( "replaceWhitespaceWith =" + replaceWhitespaceWith );

    String ignoreCaseSt = params.get( "ignoreCase" );
    if (ignoreCaseSt != null && ignoreCaseSt.equalsIgnoreCase( "false" )) {
      ignoreCase = false;
    }

    Log.info( "ignoreCase =" + ignoreCaseSt );

  }

  @Override
  public QParser createParser( String qStr, SolrParams localParams, SolrParams params,
			                   SolrQueryRequest req) {
    Log.info( "createParser" );
    ModifiableSolrParams modparams = new ModifiableSolrParams( params );
    String modQ = filter( qStr );
    Log.info( "***** modQ = "  + modQ );
    modparams.set( "q", modQ );
    return super.createParser(modQ, localParams, modparams, req);
    		
//    		req.getCore().getQueryPlugin( parserImpl )
//                        .createParser(modQ, localParams, modparams, req);
  }

  private String filter( String qStr ) {	
    // 1) collapse " :" to ":" to protect field names
    // 2) expand ":" to ": " to free terms from field names
    // 3) expand "+" to "+ " to free terms from "+" operator
    // 4) expand "-" to "- " to free terms from "-" operator
	// 5) Autophrase with whitespace tokenizer
	// 6) collapse "+ " and "- " to "+" and "-" to glom operators.
		
    String query = qStr;
    while( query.contains( " :" ))
      query = query.replaceAll( "\\s:", ": " );

    query = query.replaceAll( "\\+", "+ " );
    query = query.replaceAll( "\\-", "- " );
    
    if (ignoreCase) {
      query = query.replaceAll( "AND", "&&" );
      query = query.replaceAll( "OR", "||" );
    }
        
    try {
      query = autophrase( query );
    }
    catch (IOException ioe ) {  }
        
    query = query.replaceAll( "\\+ ", "+" );
    query = query.replaceAll( "\\- ", "-" );
    
    if (ignoreCase) {
      query = query.replaceAll( "&&", "AND" );
      query = query.replaceAll( "\\|\\|", "OR" );
    }
		
    return query;
  }
	
  private String autophrase( String input ) throws IOException {
    WhitespaceTokenizer wt = new WhitespaceTokenizer(org.apache.lucene.util.Version.LUCENE_48, new StringReader( input ));
    TokenStream ts = wt;
    if (ignoreCase) {
      ts = new LowerCaseFilter(org.apache.lucene.util.Version.LUCENE_48, wt );
    }
    AutoPhrasingTokenFilter aptf = new AutoPhrasingTokenFilter( ts, phraseSets, false );
    if (replaceWhitespaceWith != 0)
    	aptf.setReplaceWhitespaceWith( new Character( replaceWhitespaceWith ) );
    CharTermAttribute term = aptf.addAttribute(CharTermAttribute.class);
    aptf.reset();
        
    StringBuffer strbuf = new StringBuffer( );
    while( aptf.incrementToken( )) {
      strbuf.append( term.toString( ) ).append( " " );
    }
        
    return strbuf.toString();
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (phraseSetFiles != null) {
      phraseSets = getWordSet(loader, phraseSetFiles, true );
    }
  }
	
  private CharArraySet getWordSet( ResourceLoader loader,
		                           String wordFiles, boolean ignoreCase)
		                           throws IOException {
    List<String> files = splitFileNames(wordFiles);
	CharArraySet words = null;
    if (files.size() > 0) {
      // default stopwords list has 35 or so words, but maybe don't make it that
      // big to start
      words = new CharArraySet( org.apache.lucene.util.Version.LUCENE_48, files.size() * 10, ignoreCase);
      for (String file : files) {
        List<String> wlist = getLines(loader, file.trim());
    	words.addAll(StopFilter.makeStopSet(org.apache.lucene.util.Version.LUCENE_48, wlist, ignoreCase));
      }
    }
    return words;
  }
	
  private List<String> getLines(ResourceLoader loader, String resource) throws IOException {
	return WordlistLoader.getLines(loader.openResource(resource), StandardCharsets.UTF_8);
  }

  private List<String> splitFileNames(String fileNames) {
    if (fileNames == null)
      return Collections.<String>emptyList();

    List<String> result = new ArrayList<>();
    for (String file : fileNames.split("(?<!\\\\),")) {
      result.add(file.replaceAll("\\\\(?=,)", ""));
    }

    return result;
  }
}
