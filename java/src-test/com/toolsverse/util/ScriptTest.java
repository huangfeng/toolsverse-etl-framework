/*
 * UtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.script.Bindings;

import org.junit.Test;

/**
 * ScriptTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ScriptTest
{
    @Test
    public void testAny2Js()
    {
        assertTrue("var value;value=true;".equals(Script.any2Js(null)));
        assertTrue("var value;value=true;".equals(Script.any2Js("  ")));
        
        assertTrue("var value;value=abc;".equals(Script.any2Js(" abc;  ")));
        
        assertTrue("var value;value=ab == xyz;".equals(Script
                .any2Js(" ab == xyz  ")));
        
        assertTrue("import static java.lang.Math.*;var value;value=abc;"
                .equals(Script.any2Js(" abc;  ",
                        "import static java.lang.Math.*")));
        
        assertTrue("import static java.lang.Math.*;var value;value=abc;"
                .equals(Script.any2Js(" abc;  ",
                        "import static java.lang.Math.*;")));
        
    }
    
    @Test
    public void testEval()
        throws Exception
    {
        Script script = new Script();
        
        String code = Script.any2Js(Script.sql2Java("(1 = 3) or (2 < 7)"));
        
        assertTrue("var value;value=(1 == 3) || (2 < 7);".equals(code));
        
        assertTrue(script.compile(null, "test", code, "JavaScript"));
        
        assertTrue(Boolean.TRUE.equals(script.eval(null, null, "test",
                "JavaScript")));
        
        code = Script.any2Js(Script.sql2Java("(1 < 3) and (2 > 7)"));
        
        assertTrue("var value;value=(1 < 3) && (2 > 7);".equals(code));
        
        assertTrue(script.compile(null, "test2", code, "JavaScript"));
        
        assertTrue(Boolean.FALSE.equals(script.eval(null, null, "test2",
                "JavaScript")));
    }
    
    @Test
    public void testEvalWithVars()
        throws Exception
    {
        Script script = new Script();
        
        String code = Script.any2Js(Script.sql2Java("(abc = 3) or (xyz < 7)"));
        
        assertTrue("var value;value=(abc == 3) || (xyz < 7);".equals(code));
        
        LinkedHashMap<String, String> vars = Script.getVariables(code);
        
        assertNotNull(vars);
        assertTrue(vars.size() == 4);
        
        assertTrue(script.compile(null, "test", code, "JavaScript"));
        
        Bindings bindings = script.getBindings(null, "JavaScript");
        
        assertNotNull(bindings);
        
        bindings.put("abc", 3);
        bindings.put("xyz", 8);
        
        assertTrue(Boolean.TRUE.equals(script.eval(null, bindings, "test",
                "JavaScript")));
        
        bindings = script.getBindings(null, "JavaScript");
        
        assertNotNull(bindings);
        
        bindings.put("abc", 4);
        bindings.put("xyz", 8);
        
        assertTrue(Boolean.FALSE.equals(script.eval(null, bindings, "test",
                "JavaScript")));
    }
    
    @Test
    public void testGetVariables()
    {
        LinkedHashMap<String, String> vars = Script.getVariables(null);
        
        assertNotNull(vars);
        assertTrue(vars.size() == 0);
        
        vars = Script.getVariables("   ");
        
        assertNotNull(vars);
        assertTrue(vars.size() == 0);
        
        vars = Script
                .getVariables(" abc='124' && \"very long var name\" || \"this var\" && (x-y==123 || mac_12=pc || (x+z == 18))");
        
        assertNotNull(vars);
        
        assertTrue(vars.containsKey("abc"));
        assertTrue(vars.containsKey("x"));
        assertTrue(vars.containsKey("y"));
        assertTrue(vars.containsKey("mac_12"));
        assertTrue(vars.containsKey("pc"));
        assertTrue(vars.containsKey("z"));
        assertTrue(vars.containsKey("very long var name"));
        assertTrue(vars.containsKey("this var"));
    }
    
    @Test
    public void testHasCompiledCode()
        throws Exception
    {
        Script script = new Script();
        
        assertTrue(!script.hasCompiledCode(null, "test"));
        
        String code = Script.any2Js(Script.sql2Java("(1 = 3) or (2 < 7)"));
        
        assertTrue(script.compile(null, "test", code, "JavaScript"));
        
        assertTrue(script.hasCompiledCode(null, "test"));
    }
    
    @Test
    public void testParseFunctions()
        throws Exception
    {
        assertTrue("sum(abc)".equals(Script.parseFunctions("sum(abc)", null)));
        assertTrue("sum(abc)".equals(Script.parseFunctions("sum(abc)",
                new HashMap<String, String>())));
        assertTrue("  ".equals(Script.parseFunctions("  ", null)));
        assertNull(Script.parseFunctions(null, null));
        
        Map<String, String> patterns = new HashMap<String, String>();
        
        patterns.put("sum", "com.tolsverse.Stat.Sum(field, context)");
        patterns.put("avg", "com.tolsverse.Stat.Avg(field, context)");
        
        assertTrue("com.tolsverse.Stat.Sum(field,context) + com.tolsverse.Stat.Avg(field,context)"
                .equals(Script.parseFunctions("sum(abc) + avg(xyz)", patterns)));
        
        patterns.clear();
        
        patterns.put("sum", "com.tolsverse.Stat.Sum(?, context)");
        patterns.put("avg", "com.tolsverse.Stat.Avg(?, context)");
        
        assertTrue("com.tolsverse.Stat.Sum(abc,context) + com.tolsverse.Stat.Avg(xyz,context)"
                .equals(Script.parseFunctions("sum(abc) + avg(xyz)", patterns)));
        
        assertTrue("com.tolsverse.Stat.Sum(\"abc mnm\",context) + com.tolsverse.Stat.Avg(\"xyz mnm\",context)"
                .equals(Script.parseFunctions(
                        "sum(\"abc mnm\") + avg(\"xyz mnm\")", patterns)));
        
    }
    
    @Test
    public void testSql2Java()
    {
        assertTrue(Utils.isNothing(Script.sql2Java(null)));
        
        assertTrue(" (1 > 0 && 2 < 3) || a == '124'".equals(Script
                .sql2Java(" (1 > 0 aNd 2 < 3) Or a = '124'")));
        
        assertTrue(" ((1 > 0)&& 2 < 3) || a == '124'".equals(Script
                .sql2Java(" ((1 > 0)aNd 2 < 3) Or a = '124'")));
        
        assertTrue("! (a == b)".equals(Script.sql2Java("not (a = b)")));
        
        assertTrue("a!=b".equals(Script.sql2Java("a<>b")));
        
        assertTrue("a<=b".equals(Script.sql2Java("a<=b")));
        
        assertTrue("a>=b".equals(Script.sql2Java("a>=b")));
        
    }
    
}
