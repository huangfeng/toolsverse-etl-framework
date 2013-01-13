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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * UtilsTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class UtilsTest
{
    private static final String INSERT = "insert into test%1\n"
            + "(field1, field2, field3)\n" + "values (1, 1, 1);\n";
    
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
    }
    
    @Test
    public void testAge()
    {
        Date now = Utils.str2Date("01/02/2010", null, "MM/dd/yyyy");
        
        Date born = Utils.str2Date("01/02/2008", null, "MM/dd/yyyy");
        
        assertTrue(24 == Utils.getAge(born, now, false));
        
        born = Utils.str2Date("01/02/1999", null, "MM/dd/yyyy");
        
        assertTrue(132 == Utils.getAge(born, now, false));
        
        now = Utils.str2Date("02/02/1999", null, "MM/dd/yyyy");
        born = Utils.str2Date("07/23/1988", null, "MM/dd/yyyy");
        
        assertTrue(127 == Utils.getAge(born, now, false));
        
        assertTrue(-1 == Utils.getAge(now, born, false));
        
        now = Utils.str2Date("10/02/1988", null, "MM/dd/yyyy");
        born = Utils.str2Date("07/23/1988", null, "MM/dd/yyyy");
        
        assertTrue(3 == Utils.getAge(born, now, false));
        
        now = Utils.str2Date("10/02/1988", null, "MM/dd/yyyy");
        born = Utils.str2Date("07/23/1988", null, "MM/dd/yyyy");
        
        assertTrue(0 == Utils.getAge(born, now, true));
        
        born = Utils.str2Date("07/23/1981", null, "MM/dd/yyyy");
        assertTrue(7 == Utils.getAge(born, now, true));
    }
    
    @Test
    public void testBooleanConverter()
    {
        assertTrue(Utils.str2Boolean("y", Boolean.FALSE).equals(Boolean.TRUE));
        assertTrue(Utils.str2Boolean("n", Boolean.TRUE).equals(Boolean.FALSE));
        
        assertTrue(Utils.str2Boolean("yes", Boolean.FALSE).equals(Boolean.TRUE));
        assertTrue(Utils.str2Boolean("no", Boolean.TRUE).equals(Boolean.FALSE));
        
        assertTrue(Utils.str2Boolean("true", Boolean.FALSE)
                .equals(Boolean.TRUE));
        assertTrue(Utils.str2Boolean("false", Boolean.TRUE).equals(
                Boolean.FALSE));
        
        assertTrue(Utils.str2Boolean("1", Boolean.FALSE).equals(Boolean.TRUE));
        assertTrue(Utils.str2Boolean("0", Boolean.TRUE).equals(Boolean.FALSE));
        
        assertTrue(Utils.str2Boolean("xxx", Boolean.TRUE).equals(Boolean.TRUE));
        
        assertTrue(Utils.str2Boolean("yyy", null) == null);
    }
    
    @Test
    public void testBytes2Hex()
    {
        assertNull(Utils.bytes2Hex(null));
        
        String hexStr = Utils.bytes2Hex("123".getBytes());
        
        assertNotNull(hexStr);
        
        assertTrue(hexStr.equals(Utils.bytes2Hex(Utils.hex2Bytes(hexStr))));
    }
    
    @Test
    public void testCallStatic()
    {
        assertTrue(Utils.getShellExt().equals(
                Utils.callStatic(Utils.class.getName(), "getShellExt")));
    }
    
    @Test
    public void testCapFirtsChar()
    {
        assertTrue(Utils.capFirstChar(null) == null);
        
        assertTrue("   ".equals(Utils.capFirstChar("   ")));
        
        assertTrue("This is a test"
                .equals(Utils.capFirstChar("this is a test")));
        
        assertTrue("  this is a test  ".equals(Utils
                .capFirstChar("  this is a test  ")));
    }
    
    @Test
    public void testCompareDate()
    {
        Date date1 = Utils.str2Date("01/02/2010 04:05", null,
                "MM/dd/yyyy hh:mm");
        
        Date date2 = Utils.str2Date("01/02/2010 07:30", null,
                "MM/dd/yyyy hh:mm");
        
        assertTrue(Utils.compareDate(date1, date2) == 0);
        
        date2 = Utils.str2Date("01/01/2010 07:30", null, "MM/dd/yyyy hh:mm");
        
        assertTrue(Utils.compareDate(date1, date2) == 1);
        
        date2 = Utils.str2Date("01/05/2010 07:30", null, "MM/dd/yyyy hh:mm");
        
        assertTrue(Utils.compareDate(date1, date2) == -1);
    }
    
    @Test
    public void testCompareTo()
    {
        assertTrue(Utils.compareTo(null, null) == 0);
        assertTrue(Utils.compareTo(1, null) > 0);
        assertTrue(Utils.compareTo(null, 1) < 0);
        assertTrue(Utils.compareTo(1, 1) == 0);
        assertTrue(Utils.compareTo(2, 1) > 0);
        assertTrue(Utils.compareTo(1, 2) < 0);
        assertTrue(Utils.compareTo(2.5, 2.3) > 0);
        assertTrue(Utils.compareTo(2.95, 3) < 0);
        
        assertTrue(Utils.compareTo("2.95", 3) < 0);
        
        assertTrue(Utils.compareTo("3.95", 3) > 0);
        
        Date date = new Date();
        
        assertTrue(Utils.compareTo(date, date) == 0);
        
        assertTrue(Utils.compareTo(date, new Date(date.getTime() + 10000)) < 0);
        
        assertTrue(Utils.compareTo(new Date(date.getTime() + 10000), date) > 0);
    }
    
    @Test
    public void testConcatArrays()
    {
        String[] first = {"1", "2", "3"};
        
        String[] second = {"4", "5"};
        
        String[] third = {"6", "7", "8", "9"};
        
        first = Utils.concatArrays(first, second, third);
        
        assertTrue(first.length == 9);
        
        for (int i = 0; i < first.length; i++)
            assertTrue(first[i].equals(String.valueOf(i + 1)));
    }
    
    @Test
    public void testContact()
    {
        String start = "";
        
        start = Utils.concat(start, "1", ",");
        
        assertTrue("1".equals(start));
        
        start = Utils.concat(start, "2", ",");
        
        assertTrue("1,2".equals(start));
    }
    
    @Test
    public void testDateConverter()
    {
        assertTrue(Utils.date2Str(
                Utils.str2Date("01/02/2010", null, "MM/dd/yyyy"), "MM/dd/yyyy")
                .equals("01/02/2010"));
    }
    
    @Test
    public void testDecode()
    {
        Integer value = 123;
        
        assertTrue("123".equals(Utils.decode(value, 234, "234", 123, "123")));
        
        value = 234;
        
        assertTrue("234".equals(Utils.decode(value, 234, "234", 123, "123")));
        
        assertTrue("abc".equals(Utils.decode("abc")));
        
        value = null;
        
        assertTrue("this is a null".equals(Utils.decode(value, 234, "234", 123,
                "123", null, "this is a null")));
    }
    
    @Test
    public void testDecodeArray()
    {
        Integer value = 123;
        
        assertTrue("123".equals(Utils.decodeArray(new Object[] {value, 234,
                "234", 123, "123"})));
        
        value = 234;
        
        assertTrue("234".equals(Utils.decodeArray(new Object[] {value, 234,
                "234", 123, "123"})));
        
        assertTrue("abc".equals(Utils.decodeArray(new Object[] {"abc"})));
        
        value = null;
        
        assertTrue("this is a null".equals(Utils.decodeArray(new Object[] {
                value, 234, "234", 123, "123", null, "this is a null"})));
    }
    
    @Test
    public void testEquals()
    {
        assertTrue(Utils.equals(1, 1));
        assertTrue(Utils.equals(1.2, 1.2));
        assertTrue(Utils.equals("abc", "abc"));
        
        assertTrue(!Utils.equals("ab", "abc"));
        assertTrue(!Utils.equals(1, 2));
        assertTrue(!Utils.equals(null, ""));
        
        assertTrue(Utils.equals(null, "", true));
        assertTrue(!Utils.equals(null, "", false));
    }
    
    @Test
    public void testFilter()
    {
        String value = Utils.filter("abc ddd mmm kkk lll", new String[] {"ddd",
                "kkk"}, new String[] {"11", "22"});
        
        assertTrue("abc 11 mmm 22 lll".equals(value));
    }
    
    @Test
    public void testFindStrings()
    {
        assertTrue(Utils.findAndReplaceRegexp("This is A TeSt string",
                "a test", "not empry", true).equals("This is not empry string"));
        
        assertTrue(!Utils.findAndReplaceRegexp("This is A TeSt string",
                "a test", "not empry", false)
                .equals("This is not empry string"));
        
        assertTrue(Utils.findAndReplace("This is A TeSt string", "a test",
                "not empry", true).equals("This is not empry string"));
        
        assertTrue(!Utils.findAndReplace("This is A TeSt string", "a test",
                "not empry", false).equals("This is not empry string"));
        
    }
    
    @Test
    public void testGetDateDiff()
    {
        Date endDate = new GregorianCalendar(2000, 11, 31, 23, 59).getTime();
        Date startDate = new GregorianCalendar(2000, 11, 31, 23, 10).getTime();
        
        assertTrue(49 == Utils.getDateDiff(endDate, startDate, Calendar.MINUTE));
        
        startDate = new GregorianCalendar(2000, 11, 31, 20, 10).getTime();
        assertTrue(3 == Utils.getDateDiff(endDate, startDate, Calendar.HOUR));
        
        startDate = new GregorianCalendar(2000, 11, 21, 23, 10).getTime();
        assertTrue(10 == Utils.getDateDiff(endDate, startDate, Calendar.DATE));
        
        startDate = new GregorianCalendar(2000, 7, 21, 23, 10).getTime();
        assertTrue(4 == Utils.getDateDiff(endDate, startDate, Calendar.MONTH));
        
        startDate = new GregorianCalendar(2000, 1, 21, 23, 10).getTime();
        assertTrue(10 == Utils.getDateDiff(endDate, startDate, Calendar.MONTH));
        
        startDate = new GregorianCalendar(1998, 11, 21, 23, 10).getTime();
        assertTrue(2 == Utils.getDateDiff(endDate, startDate, Calendar.YEAR));
        
        endDate = new Date();
        startDate = new Date(endDate.getTime() - 25000);
        assertTrue(25 == Utils.getDateDiff(endDate, startDate, Calendar.SECOND));
        
        assertTrue(0 == Utils.getDateDiff(null, startDate, Calendar.YEAR));
        
        assertTrue(0 == Utils.getDateDiff(startDate, null, Calendar.YEAR));
        
        assertTrue(0 == Utils.getDateDiff(startDate, endDate, -100));
    }
    
    @Test
    public void testGetFunctionParams()
    {
        String[] value = Utils.getFunctionParams("decode", "decode(1,2,3,4)");
        
        assertNotNull(value);
        
        assertTrue(value.length == 4);
        
        value = Utils.getFunctionParams("decode", "decode(1,2)");
        
        assertNotNull(value);
        
        assertTrue(value.length == 2);
        
        value = Utils.getFunctionParams("decode", "decode(1)");
        
        assertNotNull(value);
        
        assertTrue(value.length == 1);
        
        value = Utils.getFunctionParams("abc", "decode(1,2,3,4)");
        
        assertTrue(value == null);
        
        value = Utils.getFunctionParams(null, "decode(1,2,3,4)");
        
        assertTrue(value == null);
        
        value = Utils.getFunctionParams("decode", "ecode(1,2,3,4)");
        
        assertTrue(value == null);
        
        value = Utils.getFunctionParams("decode", null);
        
        assertTrue(value == null);
    }
    
    @Test
    public void testGetParam()
    {
        String value = Utils.getParam("abc=aaa;xyz=123;fff=777", "xyz");
        
        assertNotNull(value);
        assertTrue("123".equals(value));
        
        assertTrue(Utils.getParam(null, "xyz") == null);
        assertTrue(Utils.getParam("abc=aaa;xyz=123;fff=777", null) == null);
        assertTrue(Utils.getParam("abc=aaa;xyz=123;fff=777", "mmm") == null);
    }
    
    @Test
    public void testGetParamFromMap()
    {
        Map<String, String> params = new HashMap<String, String>();
        
        params.put("param1", "value1");
        params.put("param2", "value2");
        
        assertTrue("value1".equals(Utils.getParamFromMap("param1", params,
                "xxx")));
    }
    
    @Test
    public void testGetProps()
    {
        Properties props = Utils.getProperties("abc=aaa;xyz=123");
        
        assertNotNull(props);
        assertTrue(props.size() == 2);
        assertTrue("123".equals(props.get("xyz")));
        
        props = Utils.getProperties("");
        
        assertNotNull(props);
        assertTrue(props.size() == 0);
        
        props = Utils.getProperties(null);
        
        assertNotNull(props);
        assertTrue(props.size() == 0);
        
        props = Utils.getProperties("  ");
        
        assertNotNull(props);
        assertTrue(props.size() == 0);
        
        props = Utils.getProperties("abc=';';xyz=123");
        
        assertNotNull(props);
        assertTrue(props.size() == 2);
        assertTrue(";".equals(props.get("abc")));
        assertTrue("123".equals(props.get("xyz")));
    }
    
    @Test
    public void testGetPropsMap()
    {
        LinkedHashMap<String, String> props = Utils
                .getPropertiesMap("abc=aaa;xyz=123");
        
        int index = 0;
        for (String name : props.keySet())
        {
            if (index == 0)
                assertTrue("abc".equals(name));
            else if (index == 1)
                assertTrue("xyz".equals(name));
            
            index++;
        }
        
        assertNotNull(props);
        assertTrue(props.size() == 2);
        assertTrue("123".equals(props.get("xyz")));
        
        props = Utils.getPropertiesMap("");
        
        assertNotNull(props);
        assertTrue(props.size() == 0);
        
        props = Utils.getPropertiesMap(null);
        
        assertNotNull(props);
        assertTrue(props.size() == 0);
        
        props = Utils.getPropertiesMap("  ");
        
        assertNotNull(props);
        assertTrue(props.size() == 0);
        
        props = Utils.getPropertiesMap("abc=';';xyz=123");
        
        assertNotNull(props);
        assertTrue(props.size() == 2);
        assertTrue(";".equals(props.get("abc")));
        assertTrue("123".equals(props.get("xyz")));
    }
    
    @Test
    public void testGetSeries()
    {
        String[] series = Utils.getSeries(1, 50, 10);
        
        assertNotNull(series);
        
        assertTrue(series.length == 50);
        
        assertTrue(series[0].equals("10"));
        
        assertTrue(series[series.length - 1].equals("500"));
    }
    
    @Test
    public void testGetVariables()
    {
        LinkedHashMap<String, TypedKeyValue<String, String>> vars = Utils
                .getVariables(null, '<', '>');
        
        assertNotNull(vars);
        assertTrue(vars.size() == 0);
        
        vars = Utils.getVariables("   ", '<', '>');
        
        assertNotNull(vars);
        assertTrue(vars.size() == 0);
        
        vars = Utils.getVariables(
                "jdbc:oracle:thin:@//<host>:<port>/<service_name>", '<', '>');
        
        assertTrue(vars.size() == 3);
        
        int index = 0;
        for (String key : vars.keySet())
        {
            if (index == 0)
            {
                assertTrue("host".equals(key));
                
                assertTrue("Host".equals(vars.get(key).getKey()));
            }
            else if (index == 1)
            {
                assertTrue("port".equals(key));
                
                assertTrue("Port".equals(vars.get(key).getKey()));
            }
            if (index == 2)
            {
                assertTrue("service_name".equals(key));
                
                assertTrue("Service name".equals(vars.get(key).getKey()));
            }
            
            index++;
        }
        
    }
    
    @Test
    public void testHex2Bytes()
    {
        assertNull(Utils.hex2Bytes(null));
        
        byte[] hex = Utils.hex2Bytes("123");
        
        assertNotNull(hex);
    }
    
    @Test
    public void testIdDateOnly()
    {
        assertTrue(!Utils.isDateOnly(null, null));
        
        assertTrue(!Utils.isDateOnly(new Date(), null));
        
        assertTrue(Utils.isDateOnly(Utils.setTimeToMidnight(new Date()), null));
        
        assertTrue(Utils.isDateOnly(Utils.setTimeToMidnight(new Date()),
                Utils.getDefaultTimeZone()));
    }
    
    @Test
    public void testIsBoolean()
    {
        assertTrue(!Utils.isBoolean("yyy"));
        
        assertTrue(!Utils.isBoolean(null));
        
        assertTrue(!Utils.isBoolean("  "));
        
        assertTrue(!Utils.isBoolean(""));
        
        assertTrue(Utils.isBoolean("true"));
        
        assertTrue(Utils.isBoolean("Yes"));
        
        assertTrue(Utils.isBoolean("No"));
        
        assertTrue(Utils.isBoolean("false"));
        
        assertTrue(Utils.isBoolean("TrUe"));
    }
    
    @Test
    public void testIsDecimal()
    {
        assertTrue(!Utils.isDecimal(null));
        
        assertTrue(!Utils.isDecimal(123));
        
        assertTrue(!Utils.isDecimal(123.0));
        
        assertTrue(!Utils.isDecimal(1.00004576E10));
        
        assertTrue(Utils.isDecimal(123.1));
        
        assertTrue(Utils.isDecimal(0.11));
        
        assertTrue(!Utils.isDecimal(0));
        
        assertTrue(!Utils.isDecimal(-0));
        
        assertTrue(Utils.isDecimal(-20.11));
    }
    
    @Test
    public void testIsInteger()
    {
        assertTrue(Utils.isInteger(1.000));
        
        assertTrue(Utils.isInteger(1000));
        
        assertTrue(!Utils.isInteger(1000.23));
        
        assertTrue(!Utils.isInteger(1000.00023));
    }
    
    @Test
    public void testIsNothing()
    {
        assertTrue(Utils.isNothing(" "));
        assertTrue(!Utils.isNothing("a"));
        assertTrue(Utils.isNothing(""));
        assertTrue(Utils.isNothing("\t"));
    }
    
    @Test
    public void testIsNumber()
    {
        assertTrue(Utils.isNumber("123"));
        assertTrue(Utils.isNumber("-123"));
        assertTrue(Utils.isNumber("123.1"));
        assertTrue(Utils.isNumber("123.123"));
        assertTrue(Utils.isNumber("-12.12"));
        assertTrue(Utils.isNumber("32."));
        assertTrue(Utils.isNumber("1.34828328348E20"));
        assertTrue(!Utils.isNumber("abc"));
        assertTrue(!Utils.isNumber("32 3232"));
        assertTrue(!Utils.isNumber("123.123.123"));
        assertTrue(!Utils.isNumber("-"));
        assertTrue(!Utils.isNumber("."));
        assertTrue(!Utils.isNumber(""));
        assertTrue(!Utils.isNumber("   "));
        assertTrue(!Utils.isUnsignedInt(null));
    }
    
    @Test
    public void testIsParticularException()
    {
        assertTrue(!Utils.isParticularException(null, "test"));
        
        assertTrue(!Utils.isParticularException(new Exception(), "test"));
        
        assertTrue(!Utils.isParticularException(new Exception(" "), "test"));
        
        assertTrue(Utils.isParticularException(new Exception("test"), "test"));
        
        assertTrue(Utils.isParticularException(new Exception(
                "this is a test exception"), "test"));
    }
    
    @Test
    public void testIsUnsignedInt()
    {
        assertTrue(Utils.isUnsignedInt("123"));
        assertTrue(!Utils.isUnsignedInt("-123"));
        assertTrue(!Utils.isUnsignedInt("abc"));
        assertTrue(!Utils.isUnsignedInt("123.1"));
        assertTrue(!Utils.isUnsignedInt(null));
    }
    
    @Test
    public void testJustGetParam()
    {
        String value = Utils.getParam("abc=aaa");
        
        assertNotNull(value);
        assertTrue("aaa".equals(value));
        
        assertTrue(Utils.getParam(null) == null);
        assertTrue(Utils.getParam("abc-aaa") == null);
    }
    
    @Test
    public void testLongestCommonSubstring()
    {
        assertTrue(Utils.isNothing(Utils.longestCommonSubstring("abc", "123")));
        
        assertTrue("ab".equals(Utils.longestCommonSubstring(
                "fsdfdgfdgfsdabsfgdshhghg", "34546654654abc657876876687")));
        
        assertTrue("123 456".equals(Utils.longestCommonSubstring(
                "dfgfgdgfd 123 456 fsdfdsfdssfdfsd", "h12123 456789543453453")));
    }
    
    @Test
    public void testNumberConverter()
    {
        assertTrue(Utils.str2Int("123", 0) == 123);
        assertTrue(Utils.str2Int("abc", 123) == 123);
        
        assertTrue(Utils.str2Long("123", 0) == 123);
        assertTrue(Utils.str2Long("abc", 123) == 123);
        
        assertTrue((Float)Utils.str2Number("11.23", null) == (float)11.23);
        assertNull(Utils.str2Number("aaa", null));
        assertTrue((Integer)Utils.str2Number("abc", 123) == 123);
        assertTrue((Integer)Utils.str2Number("12345", null) == 12345);
        assertTrue((Long)Utils.str2Number(
                new Long((Long.MAX_VALUE - 10)).toString(), null) == Long.MAX_VALUE - 10);
    }
    
    @Test
    public void testNvl()
    {
        assertTrue("".equals(Utils.nvl(" ")));
        assertTrue("".equals(Utils.nvl(null)));
        assertTrue("test".equals(Utils.nvl("test")));
        
        assertTrue("".equals(Utils.nvl(" ", "\n")));
        assertTrue("".equals(Utils.nvl(null, "\n")));
        assertTrue("test\n".equals(Utils.nvl("test", "\n")));
        
        assertTrue("xyz".equals(Utils.nvl(" ", "xyz", "\n")));
        assertTrue("abc\n".equals(Utils.nvl("abc", "xyz", "\n")));
    }
    
    @Test
    public void testOsFuncs()
    {
        if (Utils.isWindows())
            assertTrue(Utils.getShellExt(null).equals(".cmd"));
        else if (Utils.isMac())
            assertTrue(Utils.getShellExt(null).equals(".sh"));
        else
            assertTrue(Utils.getShellExt(null).equals(".sh"));
    }
    
    @Test
    public void testPad()
    {
        assertTrue("abc  ".equals(Utils.padRight("abc", 5, false)));
        assertTrue("     ".equals(Utils.padRight("", 5, false)));
        assertTrue("     ".equals(Utils.padRight(null, 5, false)));
        assertTrue("abcde".equals(Utils.padRight("abcdef", 5, true)));
        
        assertTrue("  abc".equals(Utils.padLeft("abc", 5, false)));
        assertTrue("     ".equals(Utils.padLeft("", 5, false)));
        assertTrue("     ".equals(Utils.padLeft(null, 5, false)));
        assertTrue("abcde".equals(Utils.padLeft("abcdef", 5, true)));
    }
    
    @Test
    public void testRegexpFuncs()
    {
        String value = Utils
                .removeWhiteSpace(" abc      dsadsa sdadsa  123    ");
        
        assertTrue("abc dsadsa sdadsa 123".equals(value));
    }
    
    @Test
    public void testRemoveEmptySpace()
    {
        assertTrue(Utils.removeEmptySpace(null) == null);
        
        assertTrue("".equals(Utils.removeEmptySpace(" ")));
        
        assertTrue("test".equals(Utils.removeEmptySpace(" test ")));
        
        assertTrue("test abc".equals(Utils.removeEmptySpace(" test abc ")));
        
        assertTrue("test abc".equals(Utils
                .removeEmptySpace("\n\r\ntest abc\r\n ")));
    }
    
    @Test
    public void testSetGetDate()
    {
        assertTrue(Utils.setDate(null, 123, Calendar.DATE, null) == null);
        
        Date date = new Date();
        
        assertTrue(Utils.getDate(
                Utils.setDate(date, 1997, Calendar.YEAR, null), Calendar.YEAR,
                null) == 1997);
        
        assertTrue(Utils.getDate(
                Utils.setDate(null, 1997, Calendar.YEAR, null), Calendar.YEAR,
                null) == -1);
    }
    
    @Test
    public void testSetSplit()
    {
        assertNull(Utils.setSplit(null, ","));
        
        Set<String> set = Utils.setSplit("1,2,3,4", ",");
        
        assertNotNull(set);
        
        assertTrue(set.size() == 4);
        
        assertTrue(set.contains("1"));
        assertTrue(set.contains("2"));
        assertTrue(set.contains("3"));
        assertTrue(set.contains("4"));
        assertTrue(!set.contains("5"));
    }
    
    @Test
    public void testSetVarValues()
    {
        LinkedHashMap<String, TypedKeyValue<String, String>> vars = Utils
                .getVariables(null, '<', '>');
        
        assertNotNull(vars);
        assertTrue(vars.size() == 0);
        
        vars = Utils.getVariables("   ", '<', '>');
        
        assertNotNull(vars);
        assertTrue(vars.size() == 0);
        
        String source = "jdbc:oracle:thin:@//<host>:<port>/<service_name>";
        
        vars = Utils.getVariables(source, '<', '>');
        
        assertTrue(vars.size() == 3);
        
        vars.get("host").setValue("localhost");
        vars.get("port").setValue("1521");
        vars.get("service_name").setValue("orcl1");
        
        assertTrue("jdbc:oracle:thin:@//localhost:1521/orcl1".equals(Utils
                .setVarValues(source, vars, '<', '>')));
        
        vars.get("host").setValue("c:\\abc");
        
        assertTrue("jdbc:oracle:thin:@//c:\\abc:1521/orcl1".equals(Utils
                .setVarValues(source, vars, '<', '>')));
        
    }
    
    @Test
    public void testSplit()
    {
        String[] ret = Utils.split("123;456;789", ";", null);
        
        assertNotNull(ret);
        assertTrue(ret.length == 3);
        assertTrue("123".equals(ret[0]));
        assertTrue("456".equals(ret[1]));
        assertTrue("789".equals(ret[2]));
        
        int[] fields = {4, 6, 5};
        
        ret = Utils.split("123 456   789  ", ";", fields);
        assertNotNull(ret);
        assertTrue(ret.length == 3);
        assertTrue("123".equals(ret[0]));
        assertTrue("456".equals(ret[1]));
        assertTrue("789".equals(ret[2]));
        
        fields = new int[] {4, 6, 2};
        
        ret = Utils.split("123 456   789  ", ";", fields);
        assertNotNull(ret);
        assertTrue(ret.length == 3);
        assertTrue("123".equals(ret[0]));
        assertTrue("456".equals(ret[1]));
        assertTrue("78".equals(ret[2]));
        
        fields = new int[] {4, 6, 20};
        
        ret = Utils.split("123 456   789  ", ";", fields);
        assertNotNull(ret);
        assertTrue(ret.length == 3);
        assertTrue("123".equals(ret[0]));
        assertTrue("456".equals(ret[1]));
        assertTrue("789".equals(ret[2]));
    }
    
    @Test
    public void testSplitLines()
    {
        List<TypedKeyValue<String, Integer>> assembledScripts = new ArrayList<TypedKeyValue<String, Integer>>();
        
        int count = 170;
        int maxSize = 110;
        String value = null;
        
        for (int i = 0; i < count; i++)
            value = Utils.splitLines(assembledScripts,
                    Utils.format(INSERT, new String[] {String.valueOf(i + 1)}),
                    maxSize, "\n");
        
        int blockSize = INSERT.split("\n").length + 1;
        int blockCount = maxSize / blockSize;
        
        assertTrue(!Utils.isNothing(value));
        
        assertTrue(assembledScripts.size() == count / blockCount
                + ((count % blockCount) == 0 ? 0 : 1));
    }
    
    @Test
    public void testStr2Date()
    {
        assertTrue(Utils.str2Date(null, new String[] {"MMM dd yyyy"}) == null);
        
        assertTrue(Utils.str2Date("", new String[] {"MMM dd yyyy"}) == null);
        
        assertTrue(Utils.str2Date("  ", new String[] {"MMM dd yyyy"}) == null);
        
        assertTrue(Utils.date2Str(
                Utils.str2Date("Feb 26 2009", new String[] {"MMM dd yyyy"}),
                "MMM dd yyyy").equals("Feb 26 2009"));
        
        assertTrue(Utils.date2Str(
                Utils.str2Date("Feb 26 22:34", new String[] {"MMM dd HH:mm"}),
                "MMM dd HH:mm").equals("Feb 26 22:34"));
        
        assertTrue(Utils.date2Str(
                Utils.str2Date("Feb 26 22:34", new String[] {"MMM dd yyyy",
                        "MMM dd HH:mm"}), "MMM dd HH:mm")
                .equals("Feb 26 22:34"));
    }
    
    @Test
    public void testStr2Name()
    {
        assertTrue(Utils.str2Name("   ") == null);
        
        assertTrue("test".equals(Utils.str2Name("test")));
        
        assertTrue("testxml".equals(Utils.str2Name("test.xml")));
        
        assertTrue("test".equals(Utils.str2Name("  te   st  ")));
        
        assertTrue("test".equals(Utils.str2Name("-te___st---")));
        
        assertTrue("test123".equals(Utils.str2Name("test_123")));
        
        assertTrue("test123abc".equals(Utils.str2Name("test/123/abc")));
    }
    
    @Test
    public void testStr2PropsStr()
    {
        assertTrue("".equals(Utils.str2PropsStr(null, ";", "'")));
        
        assertTrue("".equals(Utils.str2PropsStr("  ", ";", "'")));
        
        assertTrue("abc='aaa';xyz='123';fff='777';".equals(Utils.str2PropsStr(
                "abc=aaa;xyz=123;fff=777", ";", "'")));
        
        assertTrue("delimiter=\";\" firstrow=\"false\" fields=\"6;12;15;8\""
                .equals(Utils.str2PropsStr(
                        "delimiter=';';firstrow=false;fields='6;12;15;8'", " ",
                        "\"")));
        
        assertTrue("abc=\"aaa\" xyz=\"123\" fff=\"777\"".equals(Utils
                .str2PropsStr("abc = aaa;xyz= 123;fff=777;", " ", "\"")));
    }
    
    @Test
    public void testStr2Regexp()
    {
        assertTrue(Utils.str2Regexp(null) == null);
        
        assertTrue("567".equals(Utils.str2Regexp("567")));
        
        assertTrue("\\s\\+str".equals(Utils.str2Regexp("\\s+str")));
    }
    
    @Test
    public void testStr2Str()
    {
        assertTrue(Utils.str2Str(null, null) == null);
        
        assertTrue("test".equals(Utils.str2Str(null, "test")));
        
        assertTrue("abc".equals(Utils.str2Str("abc", "test")));
        
        assertTrue("abc".equals(Utils.str2Str("abc", null)));
    }
    
    @Test
    public void testStrings()
    {
        assertTrue(!Utils.isNothing("abc"));
        assertTrue(Utils.isNothing(null));
        assertTrue(Utils.isNothing(""));
        assertTrue(Utils.isNothing("   "));
        
        assertTrue(Utils.isEmpty(null));
        assertTrue(Utils.isEmpty(""));
        assertTrue(!Utils.isEmpty(" "));
        
        assertTrue(Utils.makeString("abc").equals("abc"));
        assertTrue(Utils.makeString(null).equals(""));
        
        assertTrue(Utils.belongsTo("abc", "123abc456"));
        
        assertTrue(Utils.belongsTo(new String[] {"abc", "xyz", "123"}, "xyz"));
        
        assertTrue(Utils.format("this is a %1 test %2 string",
                new String[] {"123", "456"}).equals(
                "this is a 123 test 456 string"));
        
        assertTrue(Utils.trim("abcd123", 4, null).equals("abcd"));
        
        assertTrue(Utils.trim("abcd123", 4, "xyz").equals("axyz"));
        
        assertTrue(Utils.trim("abc", 4, null).equals("abc"));
        
        assertTrue(Utils.trim("abcd", 4, null).equals("abcd"));
        
        assertTrue(Utils.trim("  this     is a string  ").equals(
                "thisisastring"));
    }
    
    @Test
    public void testTokensFuncs()
    {
        String[] result = Utils.getTokensRegexp("abc;123;cdf", ";");
        
        assertNotNull(result);
        assertTrue(result.length == 3);
        assertTrue(result[1].equals("123"));
        
        result = Utils.getTokensRegexp("", ";");
        assertNotNull(result);
        assertTrue(result.length == 0);
        
        result = Utils.getTokens("abc|123|cdf", '|', 0);
        
        assertNotNull(result);
        assertTrue(result.length == 3);
        assertTrue(result[1].equals("123"));
        
        result = Utils.getTokens("", ';', 0);
        assertNotNull(result);
        assertTrue(result.length == 0);
        
        result = Utils.getTokens("abc|123|cdf", '|', 2);
        
        assertNotNull(result);
        assertTrue(result.length == 2);
        assertTrue(result[0].equals("abc"));
    }
    
    @Test
    public void testTrancateEnd()
    {
        assertTrue("abc".equals(Utils.truncEnd("abc\n", "\n")));
        assertTrue("xyz".equals(Utils.truncEnd("xyz", "m")));
    }
    
    @Test
    public void testTrimLeft()
    {
        assertTrue(Utils.trimLeft(null, 10) == null);
        
        assertTrue("567".equals(Utils.trimLeft("1234567", 3)));
    }
    
    @Test
    public void testUUIDFuncs()
    {
        assertTrue(!Utils.isNothing(Utils.getUUID()));
        
        assertTrue(!Utils.getUUID().equals(Utils.getUUIDName()));
    }
    
    @Test
    public void testValue2Type()
        throws ParseException
    {
        assertNull(Utils.value2Type(Utils.STRING, null));
        
        assertTrue("abc".equals(Utils.value2Type(Utils.STRING, "abc")));
        
        assertTrue(new Integer(123).equals(Utils.value2Type(Utils.INTEGER,
                "123")));
        
        assertTrue(new Integer(123)
                .equals(Utils.value2Type(Utils.INTEGER, 123)));
        
        assertTrue(new Long(123243423).equals(Utils.value2Type(Utils.LONG,
                123243423)));
        
        assertTrue(new Long(123243423).equals(Utils.value2Type(Utils.LONG,
                "123243423")));
        
        assertTrue(Boolean.TRUE.equals(Utils.value2Type(Utils.BOOLEAN, true)));
        
        assertTrue(Boolean.TRUE.equals(Utils.value2Type(Utils.BOOLEAN, "yes")));
        
        assertTrue(DateUtil.parse("01/01/2004").equals(
                Utils.value2Type(Utils.DATE, "01/01/2004")));
        
    }
    
}
