// (c) Copyright 2009 Cloudera, Inc.

package index;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * All tests for inverted index code
 *
 * @author aaron
 */
public final class AllTests  {

  private AllTests() { }

  public static Test suite() {
    TestSuite suite = new TestSuite("Test for inverted index");

    suite.addTestSuite(MapperTest.class);
    suite.addTestSuite(ReducerTest.class);
    suite.addTestSuite(WordFreqMapperTest.class);
    suite.addTestSuite(WordFreqReducerTest.class);
    suite.addTestSuite(BetterStringTokenizerTest.class);

    return suite;
  }

}

