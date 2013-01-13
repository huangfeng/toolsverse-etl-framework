/*
 * GenericSqlParserTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.parser;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.GenericJdbcDriver;
import com.toolsverse.util.Utils;

/**
 * GenericSqlParserTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class GenericSqlParserTest
{
    @Test
    public void testGetVariable()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        Driver driver = new GenericJdbcDriver();
        
        assertNull(parser.getVariable("test", driver));
        
        assertNull(parser.getVariable("integer_test", driver));
        
        Variable var = parser.getVariable("out_integer_test", driver);
        
        assertNotNull(var);
        
        assertTrue("test".equals(var.getName()));
        
        assertTrue(String.valueOf(Types.INTEGER).equals(var.getType()));
        
        var = parser.getVariable("out_VARCHAR_xyz", driver);
        
        assertNotNull(var);
        
        assertTrue("xyz".equals(var.getName()));
        
        assertTrue(String.valueOf(Types.VARCHAR).equals(var.getType()));
    }
    
    @Test
    public void testIsCallable()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        assertTrue(!parser.isCallable("select * from dual"));
        
        assertTrue(!parser.isCallable("create table abc (a int);"));
        
        assertTrue(parser.isCallable("create procedure xxx begin end"));
        
        assertTrue(parser.isCallable("create procedure xxx end procedure"));
        
        assertTrue(parser.isCallable("begin xxx; end;"));
    }
    
    @Test
    public void testParseParams()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        Driver driver = new GenericJdbcDriver();
        
        Map<String, List<Integer>> paramMap = new HashMap<String, List<Integer>>();
        
        parser.parseParams(
                "select * from abc /* xx = :abc*/ where xyz = :xyz and abc = :abc and mmm = :xyz",
                null, paramMap, null, driver);
        
        assertTrue(paramMap.size() == 2);
        
        assertTrue(paramMap.containsKey("xyz"));
        
        assertTrue(paramMap.get("xyz").size() == 2);
        
        assertTrue(paramMap.get("xyz").get(0) == 1);
        assertTrue(paramMap.get("xyz").get(1) == 3);
        
        assertTrue(paramMap.containsKey("abc"));
        
        paramMap = new HashMap<String, List<Integer>>();
        
        List<String> outputParams = new ArrayList<String>();
        
        parser.parseParams("begin abc = :abc; :out_integer_xyz = :mmm; end",
                null, paramMap, outputParams, driver);
        
        assertTrue(outputParams.size() == 1);
        
        assertTrue("out_integer_xyz".equals(outputParams.get(0)));
    }
    
    @Test
    public void testRemoveChars()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        assertTrue("select * from dual".equals(parser
                .removeChars("select * from dual")));
        
        assertTrue("select * from dual".equals(parser
                .removeChars("select * \rfrom \rdual")));
    }
    
    @Test
    public void testSetBindVariables()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        Map<String, Object> properties = new HashMap<String, Object>();
        
        properties.put("var1", 1);
        properties.put("var2", 2);
        
        assertTrue("select * from dual".equals(parser.setBindVariables(
                "select * from dual", properties)));
        
        assertTrue("select * from dual where x = 1 and y = 2".equals(parser
                .setBindVariables(
                        "select * from dual where x = :var1 and y = :var2",
                        properties)));
        
    }
    
    @Test
    public void testSplit()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        String sql = "select * from dual";
        
        String[] statements = parser.split(sql);
        
        assertTrue(statements.length == 1);
        
        assertTrue("select * from dual".equals(statements[0].trim()));
        
        sql = "--select * from dual";
        
        statements = parser.split(sql);
        
        assertTrue(statements.length == 1);
        
        assertTrue(Utils.isNothing(statements[0].trim()));
        
        sql = "  /* select * from dual */  ";
        
        statements = parser.split(sql);
        
        assertTrue(statements.length == 1);
        
        assertTrue(Utils.isNothing(statements[0].trim()));
        
        sql = "select * from dual /* dsdskfds;ksd;fkdfs;*/;select * from test";
        
        statements = parser.split(sql);
        
        assertTrue(statements.length == 2);
        
        assertTrue("select * from dual /* dsdskfds;ksd;fkdfs;*/"
                .equals(statements[0].trim()));
        assertTrue("select * from test".equals(statements[1].trim()));
    }
    
    @Test
    public void testStripComments()
        throws Exception
    {
        GenericSqlParser parser = new GenericSqlParser();
        
        assertTrue("select * from dual;".equals(parser
                .stripComments("select * from dual;")));
        
        assertTrue("select *  from dual;".equals(parser
                .stripComments("select * /* a;;bc */ from dual;")));
        
        String sql = "select * from dual;--xxx\n"
                + " select * \nfrom abc /* dasdsads\n djdsfkjlkdsf */ order by 1; --sfljljdfsjs";
        
        String noComments = parser.stripComments(sql);
        
        assertTrue("select * from dual; select * from abc  order by 1;"
                .equals(noComments.trim()));
        
        assertTrue(Utils.isNothing(parser
                .stripComments("/*select * from dual;/n dask;kdsa;akds */   ")));
        
        assertTrue(Utils.isNothing(parser
                .stripComments("-- adsjlkdsajlkdasjldsajdslajdks")));
        
        assertTrue("select * from dual;".equals(parser
                .stripComments("select * from dual;--ajsddasjladsljk")));
    }
    
}
