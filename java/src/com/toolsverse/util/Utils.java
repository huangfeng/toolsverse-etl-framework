/*
 * Utils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.toolsverse.resource.Resource;
import com.toolsverse.util.log.Logger;

/**
 * The collection of static methods usable from the Toolsverse Foundation
 * Framework.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public final class Utils
{
    // data type constants
    /** The Constant STRING. */
    public static final String STRING = "string";
    
    /** The Constant INTEGER. */
    public static final String INTEGER = "integer";
    
    /** The Constant LONG. */
    public static final String LONG = "long";
    
    /** The Constant NUMBER. */
    public static final String NUMBER = "number";
    
    /** The Constant DATE. */
    public static final String DATE = "date";
    
    /** The Constant TIME. */
    public static final String TIME = "time";
    
    /** The Constant TIMESTAMP. */
    public static final String TIMESTAMP = "timestamp";
    
    /** The Constant BOOLEAN. */
    public static final String BOOLEAN = "boolean";
    
    /** The Constant WINDOWS_OS. */
    private static final String WINDOWS_OS = "windows";
    
    /** The Constant NT_OS. */
    private static final String NT_OS = "nt";
    
    /** The Constant MAC_OS. */
    private static final String MAC_OS = "mac";
    
    /** Whitespace regular expression. */
    public static final String WHITESPACE = " \n\r\f\t";
    
    /** class for formatting and parsing dates in a locale-sensitive manner. */
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
    
    /** OS dependent 'end of the line' character. */
    public final static String NEWLINE = new String(
            System.getProperty("line.separator"));
    
    /** The Constant ONE_DAY. */
    public static final long ONE_DAY = 86400000;
    
    /**
     * Checks if string <code>str</code> contained in string
     * <code>belongsToStr</code>. Case insensitive.
     * 
     * @param str
     *            the string to check
     * @param belongsToStr
     *            the string which might contain <code>str</code>
     * @return true, if successful
     */
    public static boolean belongsTo(String str, String belongsToStr)
    {
        return str != null && belongsToStr != null
                && belongsToStr.toUpperCase().indexOf(str.toUpperCase()) >= 0;
    }
    
    /**
     * Checks if string <code>check</code> contained in the array
     * <code>source</code>. Case insensitive. Ignores spaces.
     * 
     * @param source
     *            the array of strings which might contain string
     *            <code>check</code>
     * @param check
     *            the string to check
     * @return true, if successful
     */
    public static boolean belongsTo(String[] source, String check)
    {
        if (source == null || source.length == 0 || isNothing(check))
            return false;
        
        for (int i = 0; i < source.length; i++)
            if (source[i].trim().equalsIgnoreCase(check.trim()))
                return true;
        
        return false;
    }
    
    /**
     * Converts array of bytes to hex string.
     * 
     * @param bytes
     *            the array of bytes
     * @return the string
     */
    public static String bytes2Hex(byte[] bytes)
    {
        if (bytes == null)
            return null;
        
        String s = new BigInteger(bytes).toString(16);
        if ((s.length() % 2) != 0)
        {
            return "0" + s;
        }
        
        return s;
    }
    
    /**
     * Dynamically calls any (including private) method <code>methodName</code>
     * on the object <code>object</code> using parameters <code>params</code>.
     * In case of any exception returns <code>null</code>.
     *
     * @param object the object containing method <code>methodName</code>
     * @param methodName the method name to call
     * @param parameterTypes the parameter types
     * @param params the array of parameters to pass to the method
     * @return the object returned by the method. Can be void.
     */
    public static Object callAnyMethod(Object object, String methodName,
            Class<?>[] parameterTypes, Object... params)
    {
        if (object == null)
            return null;
        
        try
        {
            Method method = null;
            
            if (params == null || params.length == 0)
            {
                method = object.getClass().getDeclaredMethod(methodName);
                
                method.setAccessible(true);
                
                return method.invoke(object, (Object[])null);
            }
            else
            {
                if (parameterTypes == null)
                {
                    parameterTypes = new Class<?>[params.length];
                    
                    for (int i = 0; i < params.length; i++)
                    {
                        if (params[i] != null)
                            parameterTypes[i] = params[i].getClass();
                        else
                            parameterTypes[i] = Object.class;
                    }
                }
                
                method = object.getClass().getDeclaredMethod(methodName,
                        parameterTypes);
                
                method.setAccessible(true);
                
                return method.invoke(object, params);
            }
        }
        catch (Throwable ex)
        {
            throw new RuntimeException(ex);
        }
        
    }
    
    /**
     * Dynamically calls any (including private) method <code>methodName</code>
     * on the object <code>object</code> using parameters <code>params</code>.
     * In case of any exception returns <code>null</code>.
     * 
     * @param object
     *            the object containing method <code>methodName</code>
     * @param methodName
     *            the method name to call
     * @param params
     *            the array of parameters to pass to the method
     * @return the object returned by the method. Can be void.
     */
    public static Object callAnyMethod(Object object, String methodName,
            Object... params)
    {
        return callAnyMethod(object, methodName, null, params);
    }
    
    /**
     * Dynamically calls method <code>methodName</code> on the object
     * <code>object</code> using parameters <code>params</code>. In case of any
     * exception returns <code>null</code>.
     * 
     * @param object
     *            the object containing method <code>methodName</code>
     * @param methodName
     *            the method name to call
     * @param parameterTypes
     *            the parameter types
     * @param params
     *            the array of parameters to pass to the method
     * @return the object returned by the method. Can be void.
     */
    public static Object callMethod(Object object, String methodName,
            Class<?>[] parameterTypes, Object... params)
    {
        if (object == null)
            return null;
        
        try
        {
            Method method = null;
            
            if (params == null || params.length == 0)
            {
                method = object.getClass().getMethod(methodName);
                
                return method.invoke(object, (Object[])null);
            }
            else
            {
                if (parameterTypes == null)
                {
                    parameterTypes = new Class<?>[params.length];
                    
                    for (int i = 0; i < params.length; i++)
                    {
                        if (params[i] != null)
                            parameterTypes[i] = params[i].getClass();
                        else
                            parameterTypes[i] = Object.class;
                    }
                }
                
                method = object.getClass()
                        .getMethod(methodName, parameterTypes);
                
                return method.invoke(object, params);
            }
        }
        catch (Throwable ex)
        {
            throw new RuntimeException(ex);
        }
        
    }
    
    /**
     * Dynamically calls method <code>methodName</code> on the object
     * <code>object</code> using parameters <code>params</code>. In case of any
     * exception returns <code>null</code>.
     * 
     * @param object
     *            the object containing method <code>methodName</code>
     * @param methodName
     *            the method name to call
     * @param params
     *            the array of parameters to pass to the method
     * @return the object returned by the method. Can be void.
     */
    public static Object callMethod(Object object, String methodName,
            Object... params)
    {
        return callMethod(object, methodName, null, params);
    }
    
    /**
     * Dynamically calls static method <code>methodName</code> on the class
     * <code>className</code> using parameters <code>params</code>. In case of
     * any exception returns <code>null</code>.
     * 
     * @param className
     *            the class name of the class which contains method
     *            <code>methodName</code>
     * @param methodName
     *            the method name to call
     * @param parameterTypes
     *            the parameter types
     * @param params
     *            the array of parameters to pass to the method
     * @return the object returned by the method. Can be void.
     */
    public static Object callStatic(String className, String methodName,
            Class<?>[] parameterTypes, Object... params)
    {
        try
        {
            Class<?> c = Class.forName(className);
            
            Method method = null;
            
            if (params == null || params.length == 0)
            {
                method = c.getDeclaredMethod(methodName);
                
                return method.invoke(null);
            }
            else
            {
                if (parameterTypes == null)
                {
                    parameterTypes = new Class<?>[params.length];
                    
                    for (int i = 0; i < params.length; i++)
                    {
                        if (params[i] != null)
                            parameterTypes[i] = params[i].getClass();
                        else
                            parameterTypes[i] = Object.class;
                    }
                }
                
                method = c.getDeclaredMethod(methodName, parameterTypes);
                
                return method.invoke(null, params);
            }
        }
        catch (Throwable ex)
        {
            throw new RuntimeException(ex);
        }
        
    }
    
    /**
     * Dynamically calls static method <code>methodName</code> on the class
     * <code>className</code> using parameters <code>params</code>. In case of
     * any exception returns <code>null</code>.
     * 
     * @param className
     *            the class name of the class which contains method
     *            <code>methodName</code>
     * @param methodName
     *            the method name to call
     * @param params
     *            the array of parameters to pass to the method
     * @return the object returned by the method. Can be void.
     */
    public static Object callStatic(String className, String methodName,
            Object... params)
    {
        return callStatic(className, methodName, null, params);
        
    }
    
    /**
     * Capitalizes first character of the string.<br>
     * <br>
     * 
     * <b>Examples:</b><br>
     * 
     * abc->Abc<br>
     * 1bc->1bc<br>
     * 
     * @param value
     *            the input string
     * @return the string with capitalized first character
     */
    public static String capFirstChar(String value)
    {
        if (Utils.isNothing(value))
            return value;
        
        if (value.length() == 1)
            return value.toUpperCase();
        
        return value.substring(0, 1).toUpperCase()
                + value.substring(1, value.length());
    }
    
    /**
     * Compares two dates ignoring time.
     * 
     * @param date1
     *            the date1
     * @param date2
     *            the date2
     * @return the 1 if date1 > date 2, -1 of date1 < date2 and 0 if they are
     *         equal
     */
    public static int compareDate(Date date1, Date date2)
    {
        long value = setTimeToMidnight(date1).getTime()
                - setTimeToMidnight(date2).getTime();
        
        return value > 0 ? 1 : (value < 0 ? -1 : 0);
    }
    
    /**
     * Compare two objects.
     * 
     * @param v1 the object
     * @param v2 the object to compare
     * @return positive int if v1 > v2, negative int if v2 > v1 and 0 if v1 == v2
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static int compareTo(Object v1, Object v2)
    {
        if (v1 == null && v2 == null)
            return 0;
        
        if (v1 != null && v2 == null)
            return 1;
        
        if (v1 == null && v2 != null)
            return -1;
        
        if (v1.getClass().equals(v2.getClass()))
        {
            if (v1 instanceof Comparable)
                return ((Comparable)v1).compareTo(v2);
        }
        else if (v1 instanceof Number && v2 instanceof Number)
        {
            if (((Number)v1).doubleValue() < ((Number)v2).doubleValue())
                return -1;
            if (((Number)v1).doubleValue() > ((Number)v2).doubleValue())
                return 1;
            return 0;
        }
        
        return v1.toString().compareTo(v2.toString());
    }
    
    /**
     * Concatenates strings. Puts given delimiter between strings
     *
     * @param source the source
     * @param toAdd the string to add
     * @param delim the delimiter
     * @return the string
     */
    public static String concat(String source, String toAdd, String delim)
    {
        if (Utils.isNothing(source))
            return toAdd;
        else
            return source + delim + toAdd;
    }
    
    /**
     * Concatenates any number of the arrays of the same type.
     * 
     * @param <T>
     *            the generic type
     * @param first
     *            the first array
     * @param rest
     *            the rest of the arrays
     * @return the concatenated array
     */
    public static <T> T[] concatArrays(T[] first, T[]... rest)
    {
        int totalLength = first.length;
        for (T[] array : rest)
        {
            totalLength += array.length;
        }
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (T[] array : rest)
        {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }
    
    /**
     * Converts Date to String.
     * 
     * <p>
     * Note that since a Timestamp inherits from java.util.Date, this method
     * will also work on Timestamps.
     * </p>
     * 
     * <p>
     * Known limitation: milliseconds will print as 0 if date is a
     * <code>java.sql.Timestamp</code> object, because of inheritance weirdness
     * between it and <code>java.util.Date</code>. This is a reasonably easy
     * fix, not implemented because there's no need and it would marginally slow
     * the code.
     * </p>
     * 
     * <p>
     * For example, to display a date in the format "02/13/2001 21:45:12", use
     * <code>Utils.dateToString(myDate, "MM/dd/yyyy HH:mm:ss");</code>
     * </p>
     * 
     * @param date
     *            The date to be formatted.
     * @param format
     *            The format being applied to this date. Specifications can be
     *            found in {@link java.text.SimpleDateFormat}. Alternately, you
     *            can use one of the class variables.
     * 
     * @return The date as a string, in the appropriate format.
     * 
     * @see java.sql.Timestamp
     */
    public static String date2Str(Date date, String format)
    {
        synchronized (simpleDateFormat)
        {
            if (simpleDateFormat.getCalendar().getTimeZone() == null)
                simpleDateFormat.getCalendar()
                        .setTimeZone(getDefaultTimeZone());
            
            simpleDateFormat.applyPattern(format);
            return date != null ? simpleDateFormat.format(date) : "";
        }
    }
    
    /**
     * Similar to the Oracle decode function compares first argument to each
     * value one by one. If first argument is equal to a value, then method
     * returns the corresponding result. If no match is found, then returns
     * default. If default is omitted, then returns first argument.
     * 
     * <p>
     * Utils.decode(10, 12, "a", 10, "b", "c") -> "b"
     * <p>
     * Utils.decode(10, 12, "a", 11, "b", "c") -> "c"
     * <p>
     * Utils.decode(10, 12, "a", 11, "b") -> 10
     * <p>
     * Utils.decode(10) -> 10
     * <p>
     * Utils.decode(null) -> null
     * 
     * @param arg
     *            any number of arguments
     * @return one of the arguments based on the decode logic
     */
    public static Object decode(Object... arg)
    {
        if (arg == null || arg.length == 0)
            return null;
        
        if (arg.length == 1)
            return arg[0];
        
        Object value = arg[0];
        
        int n = arg.length;
        for (int i = 1; i < n; i += 2)
        {
            int j = i + 1;
            if (j >= n)
            {
                // only the default remains
                return arg[i];
            }
            if (equals(value, arg[i]))
                return arg[j];
        }
        
        // no match found, no default is specified
        return value;
    }
    
    /**
     * Similary to the Oracle decode function compares first element of the
     * array to each value one by one. If first element of the array is equal to
     * a value, then method returns the corresponding element of the array. If
     * no match is found, then returns default. If default is omitted, then
     * returns the first element of the array.
     * 
     * <p>
     * Utils.decodeArray(new Object[] {10, 12, "a", 10, "b", "c"}) -> "b"
     * <p>
     * Utils.decodeArray(new Object[] {10, 12, "a", 11, "b", "c"}) -> "c"
     * <p>
     * Utils.decodeArray(new Object[] {10, 12, "a", 11, "b"}) -> 10
     * <p>
     * Utils.decodeArray(new Object[] {10}) -> 10
     * <p>
     * Utils.decodeArray(null) -> null
     * 
     * @param values
     *            the values
     * @return one of the elements in the array of arguments based on the
     *         decodeArray logic
     */
    public static Object decodeArray(Object[] values)
    {
        Class<?>[] argTypes = new Class[] {Object[].class};
        
        try
        {
            Method decode = Utils.class.getDeclaredMethod("decode", argTypes);
            
            return decode.invoke(null, (Object)values);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, null, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
    }
    
    /**
     * Compares two objects.
     * 
     * @param o1
     *            the first object to compare
     * @param o2
     *            the second object to compare
     * 
     * @return true, if objects are equal
     */
    public static boolean equals(Object o1, Object o2)
    {
        return (o1 == null && o2 == null)
                || (o1 != null && o2 != null && o2.getClass().isInstance(o1) && o1
                        .equals(o2));
    }
    
    /**
     * Compares two objects. If <code>ignoreNull == true</code> converts nulls
     * to the empty strings before comparing.
     * 
     * @param o1
     *            the first object to compare
     * @param o2
     *            the second object to compare
     * @param ignoreNull
     *            if <code>true</code> converst null to the empty string
     * 
     *            <p>
     *            Utils.equals("abc", "abc", true) -> true
     *            <p>
     *            Utils.equals("abc", "cba", true) -> false
     *            <p>
     *            Utils.equals(null, "", true) -> true
     *            <p>
     *            Utils.equals(null, "", false) -> false
     * 
     * @return true, if objects are equal
     */
    public static boolean equals(Object o1, Object o2, boolean ignoreNull)
    {
        boolean ret = equals(o1, o2);
        
        if (ret || !ignoreNull)
            return ret;
        
        return makeString(o1).equals(makeString(o2));
    }
    
    /**
     * Executes shell script.
     * 
     * @param cmdFileName
     *            the shell script file name
     * @return a new {@link Process} object for managing the subprocess
     * @throws Exception
     *             in case of any error
     */
    public static Process execScript(String cmdFileName)
        throws Exception
    {
        if (isWindows())
            return Runtime.getRuntime().exec("\"" + cmdFileName + "\"");
        else
            return Runtime.getRuntime().exec(new String[] {"sh", cmdFileName});
    }
    
    /**
     * Replaces all strings from the array <code>original</code> contained in
     * the string <code>value<code>
     * on the strings in the array <code>replaceOn</code>.
     * 
     * <p>
     * Utils.filter("abc ddd mmm kkk lll", new String[] {"ddd", "kkk"}, new
     * String[] {"11", "22"}) -> "abc 11 mmm 22 lll"
     * 
     * @param value
     *            the original string
     * @param original
     *            the array of strings to search
     * @param replaceOn
     *            the array of strings to replace on
     * @return the string
     */
    public static String filter(String value, String[] original,
            String[] replaceOn)
    {
        for (int i = 0; i < original.length; i++)
            value = findAndReplace(value, original[i], replaceOn[i], false);
        
        return value;
    }
    
    /**
     * Takes String and replaces the "find" string with the "replace" string.
     * 
     * @param original
     *            the original string
     * @param find
     *            the string to search
     * @param replace
     *            the string to replace on
     * @param ignoreCase
     *            if <code>true</code> the case of all characters is ignored
     *            during search
     * 
     * @return String, if no matches exist then the original string is returned
     */
    public static String findAndReplace(String original, String find,
            String replace, boolean ignoreCase)
    {
        
        if (original == null)
            return null;
        
        StringBuffer buffer = new StringBuffer(original);
        
        int match = 0;
        
        if (buffer == null || find == null || replace == null
                || find.length() <= 0)
            return original;
        
        for (int index = 0; index < buffer.length(); index++)
        {
            if (Character.toUpperCase(find.charAt(match)) == Character
                    .toUpperCase(buffer.charAt(index)))
            {
                if (ignoreCase
                        || (find.charAt(match)) == (buffer.charAt(index)))
                {
                    match++;
                    if (match == find.length())
                    {
                        buffer.replace(index - match + 1, index + 1, replace);
                        match = 0;
                        index = index + replace.length() - find.length();
                    }
                }
                else
                    match = 0;
            }
            else
                match = 0;
        }
        return buffer.toString();
    }
    
    /**
     * Takes String and replaces the "find" string with the "replace" string
     * using regular expression.
     * 
     * @param original
     *            the original string
     * @param find
     *            the string to search. Can be a regular expression
     * @param replace
     *            the string to replace on
     * @param ignoreCase
     *            if <code>true</code> the case of all characters is ignored
     *            during search
     * 
     * @return String, if no matches exist then the original string is returned
     */
    public static String findAndReplaceRegexp(String original, String find,
            String replace, boolean ignoreCase)
    {
        String regexp = str2Regexp(find);
        
        if (ignoreCase)
            regexp = "(?i)" + regexp;
        
        return original.replaceAll(regexp, replace);
    }
    
    /**
     * Takes the input string and replaces all instances of %N on the
     * corresponding value.
     * 
     * <p>
     * Utils.format("file %1 and file %2", new String[] {"abc", "xyz"}) - >
     * "file abc and file xyz"
     * <p>
     * Utils.format("file aaa and file bbb", new String[] {"abc", "xyz"}) - >
     * "file aaa and file bbb"
     * 
     * @param input
     *            the input String
     * @param params
     *            the array of parameters
     * 
     * @return String, if no matches exist then the original string is returned
     */
    public static String format(String input, String[] params)
    {
        if (input == null || params == null || params.length == 0)
            return input;
        
        for (int i = 0; i < params.length; i++)
        {
            if (params[i] == null)
                continue;
            
            input = findAndReplace(input, "%" + (i + 1), params[i], false);
        }
        
        return input;
    }
    
    /**
     * Gets the age.
     *
     * @param date the date
     * @param compareTo the compare to date
     * @param isRound if true round age to years
     * @return the age in month
     */
    public static long getAge(Date date, Date compareTo, boolean isRound)
    {
        date = setTimeToMidnight(date);
        
        compareTo = setTimeToMidnight(compareTo);
        
        int com = compareTo.compareTo(date);
        
        if (com < 0)
            return -1;
        else if (com == 0)
            return 0;
        
        long diff = 0;
        
        if (isRound)
            return getDateDiff(compareTo, date, Calendar.YEAR);
        else
            diff = getDateDiff(compareTo, date, Calendar.MONTH);
        
        return diff;
    }
    
    /**
     * Returns the value of the given calendar field using given TimeZone
     * 
     * <p>
     * Utils.getDate(Utils.str2Date("01/02/2010", null, "MM/dd/yyyy"),
     * Calendar.YEAR, null) --> 2010
     * 
     * @param date
     *            the date
     * @param field
     *            the calendar field (Calendar.DATE, Calendar.YEAR, etc)
     * @param tz
     *            the time zone
     * @return the integer value of the given calendar field
     */
    public static int getDate(Date date, int field, TimeZone tz)
    {
        if (date == null)
            return -1;
        
        Calendar cal = null;
        
        if (tz != null)
            cal = new GregorianCalendar(tz);
        else
            cal = new GregorianCalendar();
        
        cal.setTime(date);
        
        return cal.get(field);
    }
    
    /**
     * Calculates the difference between two dates using given calendar field.
     * 
     * <p>
     * Utils.getDateDiff(Utils.str2Date("01/02/2012", null, "MM/dd/yyyy"),
     * Utils.str2Date("01/02/2010", null, "MM/dd/yyyy"), Calendar.YEAR) --> 2
     * 
     * @param endDate
     *            the end date
     * @param startDate
     *            the start date
     * @param field
     *            the calendar field (Calendar.DATE, Calendar.YEAR, etc)
     * @return the difference between two dates
     */
    @SuppressWarnings("deprecation")
    public static long getDateDiff(Date endDate, Date startDate, int field)
    {
        if (endDate == null || startDate == null)
            return 0;
        
        long diff = endDate.getTime() - startDate.getTime();
        
        switch (field)
        {
            case Calendar.MILLISECOND:
                return diff;
            case Calendar.SECOND:
                return diff / 1000;
            case Calendar.MINUTE:
                return diff / 60000;
            case Calendar.HOUR:
                return diff / 3600000;
            case Calendar.DATE:
                return diff / 86400000;
            case Calendar.MONTH:
                return (endDate.getYear() - startDate.getYear()) * 12
                        + (endDate.getMonth() - startDate.getMonth());
            case Calendar.YEAR:
                return endDate.getYear() - startDate.getYear();
        }
        
        return 0;
    }
    
    /**
     * Gets the default time zone.
     * 
     * @return the default time zone
     */
    public static TimeZone getDefaultTimeZone()
    {
        return TimeZone.getDefault();
    }
    
    /**
     * Gets the array of parameters passed to the function from the string
     * representing function's call.
     * 
     * <p>
     * Utils.getFunctionParams("abc", "abc(1,2,3)") -> {1, 2, 3}
     * 
     * @param name
     *            the name of the function
     * @param source
     *            the string representing function's call
     * @return the function parameters
     */
    public static String[] getFunctionParams(String name, String source)
    {
        if (isNothing(name) || isNothing(source))
            return null;
        
        try
        {
            Matcher matcher = Pattern.compile(name + "\\((.*)\\)").matcher(
                    source);
            
            if (!matcher.matches())
                return null;
            
            String value = matcher.group(1);
            
            if (value == null)
                return null;
            
            return value.split("\\,", -1);
        }
        catch (PatternSyntaxException e)
        {
            Logger.log(Logger.SEVERE, null, Resource.ERROR_GENERAL.getValue(),
                    e);
            
            return null;
        }
    }
    
    /**
     * Converts object to the InputStream. Used to serialize objects.
     * 
     * @param value
     *            the object to serialize
     * @return the input stream from object
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static InputStream getInputStreamFromObject(Object value)
        throws IOException
    {
        ByteArrayOutputStream outStream = null;
        ObjectOutput out = null;
        
        try
        {
            if (!(value instanceof byte[]))
            {
                outStream = new ByteArrayOutputStream();
                out = new ObjectOutputStream(outStream);
                out.writeObject(value);
                return new ByteArrayInputStream(outStream.toByteArray());
            }
            else
                return new ByteArrayInputStream((byte[])value);
        }
        finally
        {
            if (out != null)
                out.close();
            if (outStream != null)
                outStream.close();
            
        }
        
    }
    
    /**
     * Converts string to the InputStream.
     * 
     * @param value
     *            String to convert
     * @return the input stream from string
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static InputStream getInputStreamFromString(String value)
        throws IOException
    {
        return new ByteArrayInputStream(makeString(value).getBytes());
    }
    
    /**
     * Gets the value of the parameter from the parameter=value string. The
     * format of the string: name1=value1
     * 
     * <p>
     * Utils.getParam("abc=aaa") -> "aaa"
     * 
     * @param paramValue
     *            the parameter=value string
     * @return the value of the parameter.
     */
    public static String getParam(String paramValue)
    {
        if (Utils.isNothing(paramValue))
            return null;
        
        int index = paramValue.indexOf('=');
        
        if (index < 0)
            return null;
        
        return paramValue.substring(index + 1);
    }
    
    /**
     * Gets the value of the parameter from the parameter=value string. The
     * format of the string: name1=value1;name2=value2;etc
     * 
     * <p>
     * Utils.getParam("abc=aaa;xyz=123;fff=777", "xyz") -> "123"
     * 
     * @param params
     *            the parameter=value string
     * @param name
     *            the name of the parameter
     * @return the value of the parameter. If there is no match returns null.
     */
    public static String getParam(String params, String name)
    {
        if (Utils.isNothing(params))
            return null;
        
        String[] pairs = params.split(";", -1);
        
        for (int i = 0; i < pairs.length; i++)
        {
            String pair = pairs[i].trim();
            
            String[] values = pair.split("=", -1);
            
            if (values[0].equalsIgnoreCase(name))
            {
                String value = null;
                
                if (values.length > 1)
                    value = values[1];
                
                return value;
            }
        }
        
        return null;
    }
    
    /**
     * Gets the value from map of parameters using given name. If value is null
     * or not found in the map returns <code>defaultValue</code>.
     * 
     * @param name
     *            the name of the parameter
     * @param params
     *            the map of the parameters
     * @param defaultValue
     *            the default value
     * @return the parameter's value from the map. If value is null or not found
     *         in the map returns <code>defaultValue</code>.
     */
    public static String getParamFromMap(String name,
            Map<String, String> params, String defaultValue)
    {
        if (name == null || params == null)
            return defaultValue;
        
        String value = params.get(name);
        
        return value != null ? value : defaultValue;
    }
    
    /**
     * Converts input string to {@link java.util.Properties}. The format of the
     * string: name1=value1;name2=value2;etc
     * 
     * <p>
     * Utils.getProperties("abc=aaa;xyz=123")
     * 
     * @param props
     *            the input string
     * @return the properties
     */
    public static Properties getProperties(String props)
    {
        Properties properties = new Properties();
        
        if (isNothing(props))
            return properties;
        
        String[] propArray = props.trim().split(";(?=([^']*'[^']*')*[^']*$)",
                -1);
        
        if (propArray == null || propArray.length == 0)
            return properties;
        
        for (int i = 0; i < propArray.length; i++)
        {
            String prop = propArray[i];
            
            if (isNothing(prop))
                continue;
            
            prop = prop.trim();
            
            String[] nameValue = prop.split("=", -1);
            
            if (nameValue == null || nameValue.length == 0)
                continue;
            
            String name = nameValue[0].trim();
            String value = null;
            
            if (nameValue.length > 1)
            {
                value = nameValue[1].trim();
                
                if (value != null)
                {
                    if (value.startsWith("'"))
                        value = value.substring(1);
                    
                    if (value.endsWith("'"))
                        value = value.substring(0, value.length() - 1);
                }
            }
            
            if (name != null && value != null)
                properties.put(name, value);
        }
        
        return properties;
    }
    
    /**
     * Converts input string to {@link java.util.Properties}. The format of the
     * string: name1=value1;name2=value2;etc
     * 
     * <p>
     * Utils.getProperties("abc=aaa;xyz=123")
     * 
     * @param props
     *            the input string
     * @return the properties
     */
    public static LinkedHashMap<String, String> getPropertiesMap(String props)
    {
        LinkedHashMap<String, String> properties = new LinkedHashMap<String, String>();
        
        if (isNothing(props))
            return properties;
        
        String[] propArray = props.trim().split(";(?=([^']*'[^']*')*[^']*$)",
                -1);
        
        if (propArray == null || propArray.length == 0)
            return properties;
        
        for (int i = 0; i < propArray.length; i++)
        {
            String prop = propArray[i];
            
            if (isNothing(prop))
                continue;
            
            prop = prop.trim();
            
            String[] nameValue = prop.split("=", -1);
            
            if (nameValue == null || nameValue.length == 0)
                continue;
            
            String name = nameValue[0].trim();
            String value = null;
            
            if (nameValue.length > 1)
            {
                value = nameValue[1].trim();
                
                if (value != null)
                {
                    if (value.startsWith("'"))
                        value = value.substring(1);
                    
                    if (value.endsWith("'"))
                        value = value.substring(0, value.length() - 1);
                }
            }
            
            if (name != null && value != null)
                properties.put(name, value);
        }
        
        return properties;
    }
    
    /**
     * Gets the series of of values between <code>start</code> and
     * <code>end</code> using multiplier <code>mult</code>.
     * 
     * <p>
     * Utils.getSeries(1, 5, 10) - > {"10", "20", "30", "40", "50"}
     * 
     * @param start
     *            the start value
     * @param end
     *            the end value
     * @param mult
     *            the multiplier
     * @return the series
     */
    public static String[] getSeries(int start, int end, int mult)
    {
        return getSeries(null, start, end, mult);
    }
    
    /**
     * Gets the series of of values between <code>start</code> and
     * <code>end</code> using multiplier <code>mult</code>. If
     * <code>first != null</code> adds it as a first element of the output
     * array.
     * 
     * <p>
     * Utils.getSeries(null, 1, 5, 10) - > {"10", "20", "30", "40", "50"}
     * <p>
     * Utils.getSeries("0", 1, 5, 10) - > {"0", "10", "20", "30", "40", "50"}
     * 
     * @param first
     *            the String which if not null will be added as as first element
     *            of the output array.
     * @param start
     *            the start value
     * @param end
     *            the end value
     * @param mult
     *            the multiplier
     * @return the series
     */
    public static String[] getSeries(String first, int start, int end, int mult)
    {
        List<String> series = new ArrayList<String>();
        
        if (!Utils.isNothing(first))
            series.add(first);
        
        for (int i = start; i <= end; i++)
            series.add(String.valueOf(i * mult));
        
        String[] ret = new String[] {};
        
        return series.toArray(ret);
    }
    
    /**
     * Gets the default shell script extension for the current OS.
     * 
     * @return the shell script extension
     */
    public static String getShellExt()
    {
        return isWindows() ? ".cmd" : (isMac() ? ".sh" : ".sh");
    }
    
    /**
     * Gets the default shell script extension for the current OS. If
     * <code>defaultExt != null</code> returns defaultExt
     * 
     * @param defaultExt
     *            the default ext
     * @return the shell script extension.
     */
    public static String getShellExt(String defaultExt)
    {
        if (!isNothing(defaultExt))
        {
            if (".".indexOf(defaultExt) == 0)
                return defaultExt;
            else
                return "." + defaultExt;
        }
        
        return getShellExt();
    }
    
    /**
     * Converts stack trace from the given Throwable to the string.
     * 
     * @param e
     *            the Throwable
     * 
     * @return the stack trace as a string
     */
    public static String getStackTraceAsString(Throwable e)
    {
        if (e == null)
            return "";
        
        ByteArrayOutputStream ostr = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(ostr));
        return ostr.toString();
    }
    
    /**
     * Splits <code>value</code> string around matches of the given
     * <code>delimiter</code> without using regular expression.
     * 
     * <p>
     * The <tt>maxTokens</tt> parameter controls the number of times the pattern
     * is applied and therefore affects the length of the resulting array. If
     * the maxTokens <i>n</i> is greater than zero then the the pattern will be
     * applied at most <i>n</i>&nbsp;-&nbsp;1 times, the array's length will be
     * no greater than <i>n</i>, and the array's last entry will contain all
     * input beyond the last matched delimiter. If <i>n</i> is non-positive then
     * the pattern will be applied as many times as possible and the array can
     * have any length. If <i>n</i> is zero then the pattern will be applied as
     * many times as possible, the array can have any length, and trailing empty
     * strings will be discarded.
     * 
     * @param value
     *            the value
     * @param delimiter
     *            the delimiter
     * @param maxTokens
     *            the max tokens
     * @return the tokens
     */
    public static String[] getTokens(String value, char delimiter, int maxTokens)
    {
        
        if (isNothing(value))
            return new String[0];
        
        String delim = String.valueOf(delimiter);
        
        StringTokenizer st = new StringTokenizer(value, delim, true);
        List<String> list = new ArrayList<String>();
        int j = 0;
        
        String lastToken = "";
        while (st.hasMoreTokens())
        {
            
            // read the next token
            String token = st.nextToken();
            
            // if we've already collected maxTokens-1 tokens, everything else
            // goes
            // into the last token.
            if (maxTokens > 0 && (j / 2) >= maxTokens - 1)
            {
                lastToken += token;
                continue;
            }
            
            token = token.trim();
            if ((j % 2) == 0)
            {
                if (token.equals(delim))
                {
                    list.add(null);
                    j++;
                }
                else
                {
                    if (token.length() == 0)
                    {
                        list.add(null);
                    }
                    else
                    {
                        list.add(token);
                    }
                }
            }
            j++;
        }
        
        if (lastToken.length() > 0)
        {
            lastToken = lastToken.trim();
            list.add(lastToken);
        }
        
        return list.toArray(new String[list.size()]);
    }
    
    /**
     * Splits <code>value</code> string around matches of the given
     * <code>delimiter</code> using regular expression. If <code>value</code> is
     * null or <code>value.length == 0</code> returns empty array.
     * 
     * @param value
     *            the string to split
     * @param delim
     *            the delimeter
     * @return the array of strings
     */
    public static String[] getTokensRegexp(String value, String delim)
    {
        if (Utils.isNothing(value))
            return new String[0];
        
        String[] ret = value.split(delim, -1);
        
        return ret;
    }
    
    /**
     * Gets the string representation of the type 4 (pseudo randomly generated)
     * UUID. The UUID is generated using a cryptographically strong pseudo
     * random number generator.
     * 
     * @return the string representation of the type 4 UUID
     */
    public static String getUUID()
    {
        return UUID.randomUUID().toString();
    }
    
    /**
     * Gets the string representation of the type 4 (pseudo randomly generated)
     * UUID and replaces all instances of "-" on "". Used mainly to asssign name
     * to the WWEB component. The UUID is generated using a cryptographically
     * strong pseudo random number generator.
     * 
     * @return the string representation of the type 4 UUID
     */
    public static String getUUIDName()
    {
        String uuid = getUUID();
        
        return uuid.replaceAll("-", "");
    }
    
    /**
     * Returns a map of variables from the string. The variable is string between brackets. 
     *
     * @param input the input string
     * @param open the open bracket
     * @param close the close bracket
     * @return the map of variables
     */
    public static LinkedHashMap<String, TypedKeyValue<String, String>> getVariables(
            String input, char open, char close)
    {
        LinkedHashMap<String, TypedKeyValue<String, String>> vars = new LinkedHashMap<String, TypedKeyValue<String, String>>();
        
        if (Utils.isNothing(input))
            return vars;
        
        Pattern pattern = Pattern.compile("(?<=\\" + open + ")([^" + close
                + "]*)(?=\\" + close + ")", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(input);
        while (matcher.find())
        {
            String var = matcher.group();
            
            if (Utils.isNothing(var) || vars.containsKey(var))
                continue;
            
            vars.put(
                    var,
                    new TypedKeyValue<String, String>(capFirstChar(var
                            .replaceAll("_", " ")), ""));
        }
        
        return vars;
    }
    
    /**
     * Checks string for whitespaces.
     * 
     * @param value
     *            the string to check
     * @return true, if string contains whitespaces
     */
    public static boolean hasWhitespace(String value)
    {
        return WHITESPACE.indexOf(value) >= 0;
    }
    
    /**
     * Converts hex string to the array of bytes.
     * 
     * @param s
     *            the input string
     * @return the array of bytes
     */
    public static byte[] hex2Bytes(String s)
    {
        if (s == null)
            return null;
        
        return new BigInteger(s, 16).toByteArray();
    }
    
    /**
     * Checks if string is boolean.
     * 
     * <p>
     * Possible values (case insensitive): Y,N,YES,NO,TRUE,FALSE
     * 
     * @param value
     *            the string to check
     * @return true, if string is boolean
     */
    public static boolean isBoolean(String value)
    {
        if (isNothing(value))
            return false;
        
        return "YES".equalsIgnoreCase(value) || "NO".equalsIgnoreCase(value)
                || "TRUE".equalsIgnoreCase(value)
                || "FALSE".equalsIgnoreCase(value)
                || "Y".equalsIgnoreCase(value) || "N".equalsIgnoreCase(value);
    }
    
    /**
     * Checks if given date is a date only.
     * 
     * @param date
     *            the date
     * @param tz
     *            the time zone
     * @return true, if is date only
     */
    public static boolean isDateOnly(Date date, TimeZone tz)
    {
        if (date == null)
            return false;
        
        Calendar cal = null;
        
        if (tz != null)
            cal = new GregorianCalendar(tz);
        else
            cal = new GregorianCalendar();
        
        cal.setTime(date);
        
        return cal.get(Calendar.HOUR_OF_DAY) == 0
                && cal.get(Calendar.MINUTE) == 0
                && cal.get(Calendar.SECOND) == 0
                && cal.get(Calendar.MILLISECOND) == 0;
        
    }
    
    /**
     * Checks if string is decimal.
     * 
     * @param value
     *            the string to check
     * @return true, if string is decimal
     */
    public static boolean isDecimal(Number value)
    {
        return value != null && Math.abs(value.doubleValue()) % 1.0 > 0;
    }
    
    /**
     * Checks if object is 'empty'. Empty object is the one which string
     * representation is either null or "".
     * 
     * @param object
     *            the object to check
     * @return true, if object is empty
     */
    public static boolean isEmpty(Object object)
    {
        return (object == null || "".equals(object.toString()));
    }
    
    /**
     * Checks if number is integer (or long).
     *
     * @param number the number
     * @return true, if number is integer (or long).
     */
    public static boolean isInteger(double number)
    {
        return Math.floor(number) == number;
    }
    
    /**
     * Checks if current OS is Mac.
     * 
     * @return true, if current OS is Mac
     */
    public static boolean isMac()
    {
        String os = System.getProperty("os.name");
        return os != null && os.toLowerCase().indexOf(MAC_OS) >= 0;
    }
    
    /**
     * Returns true if object is null, or if object.toString().trim().length()
     * == 0.
     * 
     * @param object
     *            the object to check
     * 
     * @return true, if object is 'nothing'
     */
    public static boolean isNothing(Object object)
    {
        return (object == null || object.toString().trim().length() == 0);
    }
    
    /**
     * Checks if string is number.
     * 
     * @param toTest
     *            the string to check
     * @return true, if string is number
     */
    public static boolean isNumber(String toTest)
    {
        try
        {
            if (toTest != null)
            {
                Double.parseDouble(toTest);
                
                return true;
            }
            
            return false;
        }
        catch (Exception ex)
        {
            return false;
        }
    }
    
    /**
     * Checks if <code>ex.getMessage()</code> contains <code>pattern</code>.
     * 
     * @param ex
     *            the exeption to check
     * @param pattern
     *            the pattern
     * @return true, if pattern matches
     */
    public static boolean isParticularException(Throwable ex, String pattern)
    {
        return ex != null && ex.getMessage() != null
                && ex.getMessage().indexOf(pattern) >= 0;
    }
    
    /**
     * Checks if date is a time only.
     * 
     * @param date
     *            the date
     * @param tz
     *            the time zone
     * @return true, if is time only
     */
    public static boolean isTimeOnly(Date date, TimeZone tz)
    {
        if (date == null)
            return false;
        
        Calendar cal = null;
        
        if (tz != null)
            cal = new GregorianCalendar(tz);
        else
            cal = new GregorianCalendar();
        
        cal.setTime(date);
        
        return cal.get(Calendar.YEAR) == 0 && cal.get(Calendar.MONTH) == 0
                && cal.get(Calendar.DAY_OF_MONTH) == 0;
    }
    
    /**
     * Checks if string is an unsigned int.
     * 
     * @param toTest
     *            the string to heck
     * @return true, if string is an unsigned int
     */
    public static boolean isUnsignedInt(String toTest)
    {
        try
        {
            return toTest != null && Pattern.matches("\\d+", toTest);
        }
        catch (Exception ex)
        {
            return false;
        }
        
    }
    
    /**
     * Checks if current OS is Windows.
     * 
     * @return true, if current OS is Windows
     */
    public static boolean isWindows()
    {
        String os = System.getProperty("os.name");
        return os != null
                && (os.toLowerCase().startsWith(WINDOWS_OS) || os.toLowerCase()
                        .startsWith(NT_OS));
    }
    
    /**
     * Loads properties from class path.
     *
     * @param resourceName the resource name
     * @return the properties
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static Properties loadPropsFromClassPath(String resourceName)
        throws IOException
    {
        if (resourceName == null)
            return null;
        
        InputStream inStream = null;
        
        try
        {
            Properties toProps = new Properties();
            
            inStream = Utils.class.getClassLoader().getResourceAsStream(
                    resourceName);
            
            if (inStream != null)
                toProps.load(inStream);
            else
                return null;
            
            return toProps;
        }
        finally
        {
            if (inStream != null)
                inStream.close();
            
        }
    }
    
    /**
     * Loads properties from the file.
     *
     * @param fileName the file name
     * @return the properties
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static Properties loadPropsFromFile(String fileName)
        throws IOException
    {
        if (fileName == null || !new File(fileName).exists())
            return null;
        
        FileInputStream fis = null;
        
        try
        {
            Properties toProps = new Properties();
            
            fis = new FileInputStream(fileName);
            toProps.load(fis);
            
            return toProps;
        }
        finally
        {
            if (fis != null)
                fis.close();
        }
    }
    
    /**
     * Calculates longest common substring between two strings.
     * 
     * <p>
     * longestCommonSubstring("fsdfdgfdgfsdabsfgdshhghg", "34546654654abc657876876687") --> "ab"
     * <p>
     * Utils.longestCommonSubstring("dfgfgdgfd 123 456 fsdfdsfdssfdfsd", "h12123 456789543453453") --> "123 456" 
     *
     * @param str1 the string
     * @param str2 the string
     * @return the longest common substring
     */
    public static String longestCommonSubstring(String str1, String str2)
    {
        StringBuilder sb = new StringBuilder();
        
        if (str1 == null || str1.isEmpty() || str2 == null || str2.isEmpty())
            
            return "";
        
        // ignore case
        
        str1 = str1.toLowerCase();
        
        str2 = str2.toLowerCase();
        
        // java initializes them already with 0
        
        int[][] num = new int[str1.length()][str2.length()];
        
        int maxlen = 0;
        
        int lastSubsBegin = 0;
        
        for (int i = 0; i < str1.length(); i++)
        {
            
            for (int j = 0; j < str2.length(); j++)
            {
                
                if (str1.charAt(i) == str2.charAt(j))
                {
                    
                    if ((i == 0) || (j == 0))
                        
                        num[i][j] = 1;
                    
                    else
                        
                        num[i][j] = 1 + num[i - 1][j - 1];
                    
                    if (num[i][j] > maxlen)
                    {
                        
                        maxlen = num[i][j];
                        
                        // generate substring from str1 => i
                        
                        int thisSubsBegin = i - num[i][j] + 1;
                        
                        if (lastSubsBegin == thisSubsBegin)
                        {
                            
                            // if the current LCS is the same as the last time
                            // this block ran
                            
                            sb.append(str1.charAt(i));
                            
                        }
                        else
                        {
                            
                            // this block resets the string builder if a
                            // different LCS is found
                            
                            lastSubsBegin = thisSubsBegin;
                            
                            sb = new StringBuilder();
                            
                            sb.append(str1.substring(lastSubsBegin, i + 1));
                            
                        }
                        
                    }
                    
                }
                
            }
        }
        
        return sb.toString();
        
    }
    
    /**
     * Converts object to string. Returns either <code>o.toString()</code> or ""
     * if <code>o == null || o.toString() == null</code>.
     * 
     * @param o
     *            the input object
     * @return the string
     */
    public static String makeString(Object o)
    {
        if (o == null)
            return "";
        
        String ret = o.toString();
        
        return ret != null ? ret : "";
    }
    
    /**
     * Returns either <code>value</code> or ""
     * <code>if (value.isNothing())</code>.
     * 
     * @param value
     *            the input string
     * @return the string
     */
    public static String nvl(String value)
    {
        return nvl(value, "");
    }
    
    /**
     * Returns either <code>value + delim</code> or ""
     * <code>if (value.isNothing())</code>.
     * 
     * @param value
     *            the input string
     * @param delim
     *            the string to add to the value
     * @return the string
     */
    public static String nvl(String value, String delim)
    {
        return Utils.isNothing(value) ? "" : value + delim;
    }
    
    /**
     * Returns either <code>value + delim</code> or <code>replace</code>
     * <code>if (value.isNothing())</code>.
     * 
     * @param value
     *            the input string
     * @param replace
     *            the string to replace value on
     * @param delim
     *            the string to add to the value
     * @return the string
     */
    public static String nvl(String value, String replace, String delim)
    {
        return Utils.isNothing(value) ? replace : value + delim;
    }
    
    /**
     * Creates a string by repeating <code>padChar</code> <code>repeat</code>
     * times.
     * 
     * @param repeat
     *            the number of repeats
     * @param padChar
     *            the char to repeat
     * @return the string
     * @throws IndexOutOfBoundsException
     *             if <code>repeat</code> is negative
     */
    public static String padding(int repeat, char padChar)
        throws IndexOutOfBoundsException
    {
        if (repeat < 0)
        {
            throw new IndexOutOfBoundsException(
                    "Cannot pad a negative amount: " + repeat);
        }
        final char[] buf = new char[repeat];
        for (int i = 0; i < buf.length; i++)
        {
            buf[i] = padChar;
        }
        return new String(buf);
    }
    
    /**
     * Creates a string by adding spaces to the left of the input string until
     * it's length is less than <code>n</code>. If <code>trim == true</code>
     * trims string to the <code>n</code> chars.
     * 
     * <p>
     * Utils.padLeft("abc", 5, false) -> "  abc"
     * <p>
     * Utils.padLeft("", 5, false) -> "     "
     * <p>
     * Utils.padLeft("abcdef", 5, true) -> "abcde"
     * 
     * @param value
     *            the input string
     * @param n
     *            the max length of the string
     * @param trim
     *            if <code>true</code> trims string to the <code>n</code> chars.
     * @return the string
     */
    public static String padLeft(String value, int n, boolean trim)
    {
        if (value == null)
            value = "";
        
        if (trim && value.length() > n)
            value = value.substring(0, n);
        
        return String.format("%1$" + n + "s", value);
    }
    
    /**
     * Creates a string by adding spaces to the right of the input string until
     * it's length is less than <code>n</code>. If <code>trim == true</code>
     * trims string to the <code>n</code> chars.
     * 
     * <p>
     * Utils.padRight("abc", 5, false) -> "abc  "
     * <p>
     * Utils.padRight("", 5, false) -> "     "
     * <p>
     * Utils.padRight("abcdef", 5, true) -> "abcde"
     * 
     * @param s
     *            the input string
     * @param n
     *            the max length of the string
     * @param trim
     *            if <code>true</code> trims string to the <code>n</code> chars.
     * @return the string
     */
    public static String padRight(String s, int n, boolean trim)
    {
        if (s == null)
            s = "";
        
        if (trim && s.length() > n)
            s = s.substring(0, n);
        
        return String.format("%1$-" + n + "s", s);
    }
    
    /**
     * Creates array of bytes from the input stream.
     * 
     * @param inputStream
     *            the input stream
     * @param closeStream
     *            if <code>true</code> closes inputStream
     * @return the array of bytes
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static byte[] readBytesFromInputStream(InputStream inputStream,
            boolean closeStream)
        throws IOException
    {
        ByteArrayOutputStream bos = null;
        
        try
        {
            bos = new ByteArrayOutputStream();
            int next = inputStream.read();
            while (next > -1)
            {
                bos.write(next);
                next = inputStream.read();
            }
            bos.flush();
            return bos.toByteArray();
        }
        finally
        {
            if (bos != null)
                bos.close();
            
            if (closeStream)
                inputStream.close();
        }
    }
    
    /**
     * Creates string from the input stream.
     * 
     * @param is
     *            the input stream
     * @return the string
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static String readStringFromInputStream(InputStream is)
        throws IOException
    {
        if (is == null)
            return null;
        
        StringWriter stringWriter = null;
        
        BufferedWriter writer = null;
        
        try
        {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is));
            
            stringWriter = new StringWriter();
            
            writer = new BufferedWriter(stringWriter);
            
            int c = -1;
            while ((c = reader.read()) != -1)
            {
                writer.write(c);
            }
            writer.flush();
            
            return stringWriter.toString();
        }
        finally
        {
            if (is != null)
                is.close();
            
            if (stringWriter != null)
                stringWriter.close();
            
            if (writer != null)
                writer.close();
        }
    }
    
    /**
     * Removes the empty space by calling <code>value.trim()</code>. Returns
     * null if <code>value == null</code>.
     * 
     * @param value
     *            the input string
     * @return the string
     */
    public static String removeEmptySpace(String value)
    {
        if (value == null)
            return null;
        
        value = value.trim();
        
        return value;
    }
    
    /**
     * Removes the whitespace characters.
     * 
     * <p>
     * Utils.removeWhiteSpace(" abc      dsadsa sdadsa  123    ") ->
     * "abc dsadsa sdadsa 123"
     * 
     * @param s
     *            the input string
     * @return the string
     */
    public static String removeWhiteSpace(String s)
    {
        s = Utils.findAndReplace(s, "\t", " ", false).trim();
        
        StringTokenizer st = new StringTokenizer(s, "\u0020");
        
        String result = "";
        
        while (st.hasMoreElements())
        {
            String token = st.nextToken();
            result += token + " ";
        }
        
        return result.trim();
    }
    
    /**
     * Sets the value for the specific calendar field using given TimeZone. The
     * time zone can be null in which case it will be ignored.
     * 
     * <p>
     * Date date = new Date(); Utils.getDate(Utils.setDate(date, 1997,
     * Calendar.YEAR, null), Calendar.YEAR, null) -> 1997
     * 
     * @param date
     *            the input date
     * @param value
     *            the value
     * @param field
     *            the calendar field (Calendar.YEAR, Calendar.DATE, etc)
     * @param tz
     *            the time zone
     * @return the date
     */
    public static Date setDate(Date date, int value, int field, TimeZone tz)
    {
        if (date == null)
            return null;
        
        Calendar cal = null;
        
        if (tz != null)
            cal = new GregorianCalendar(tz);
        else
            cal = new GregorianCalendar();
        
        cal.setTime(date);
        
        cal.set(field, value);
        
        return cal.getTime();
    }
    
    /**
     * Sets the date to beginning of time.
     *
     * @param date the date
     * @return the date
     */
    public static Date setDateToBeginningOfTime(Date date)
    {
        Calendar calendar = Calendar.getInstance();
        
        calendar.setTime(date);
        calendar.set(Calendar.YEAR, 0);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DATE, 0);
        
        return calendar.getTime();
    }
    
    /**
     * Splits the string using given delimiter and adds tokens to the hash set. Ignores empty strings.
     * @param toSplit the string to spllit
     * @param delim the delimiter
     * @return the set
     */
    public static Set<String> setSplit(String toSplit, String delim)
    {
        if (toSplit == null)
            return null;
        
        String[] tokens = toSplit.split(delim, -1);
        
        Set<String> set = new HashSet<String>();
        
        for (String token : tokens)
        {
            if (isNothing(token))
                continue;
            
            set.add(token);
        }
        
        return set;
    }
    
    /**
     * Sets time to midnight.
     * 
     * @param date
     *            the date
     * @return date
     */
    public static Date setTimeToMidnight(Date date)
    {
        Calendar calendar = Calendar.getInstance();
        
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        
        return calendar.getTime();
    }
    
    /**
     * Sets the variable values.
     * 
     * <pre>
     * Example:
     * 
     * setVarValues("jdbc:oracle:thin:@//<host>:<port>/<service_name>", "port", "1521", '<', '>'); --> "jdbc:oracle:thin:@//<host>:1521/<service_name>"
     * 
     * </pre>
     *
     * @param source the source
     * @param namesAndValues the names and values
     * @param open the open
     * @param close the close
     * @return the string
     */
    public static String setVarValues(
            String source,
            LinkedHashMap<String, TypedKeyValue<String, String>> namesAndValues,
            char open, char close)
    {
        if (namesAndValues == null || namesAndValues.size() == 0)
            return source;
        
        for (String name : namesAndValues.keySet())
        {
            String value = namesAndValues.get(name).getValue();
            
            if (!Utils.isNothing(value))
                source = Utils.findAndReplace(source, open + name + close,
                        value, false);
        }
        
        return source;
    }
    
    /**
     * Splits the input string using either <code>delim</code> string or if
     * <code>lengthArray != null && lengthArray.length > 0</code> lengthArray as
     * an array where each element is a field length. Used to parse special char
     * separated or fixed length data files.
     * 
     * <p>
     * Utils.split("123;456;789", ";", null) -> {"123", "456", "789"}
     * <p>
     * Utils.split("123 456   789  ", null, int[] {4, 6, 5}) -> {"123", "456",
     * "789"}
     * <p>
     * Utils.split("123 456   789  ", null, int[] {4, 6, 2}) -> {"123", "456",
     * "78"}
     * 
     * @param source
     *            the input string
     * @param delim
     *            the delimiter
     * @param lengthArray
     *            the array of
     * @return the arrays of strings
     */
    public static String[] split(String source, String delim, int[] lengthArray)
    {
        if (lengthArray == null || lengthArray.length == 0)
            return source.split(delim, -1);
        
        String[] result = new String[lengthArray.length];
        
        for (int i = 0; i < lengthArray.length; i++)
        {
            if (lengthArray[i] > source.length())
                lengthArray[i] = source.length();
            
            String value = source.substring(0, lengthArray[i]);
            
            if (value == null)
                return result;
            
            result[i] = value.trim();
            
            source = source.substring(lengthArray[i]);
        }
        
        return result;
    }
    
    /**
     * This method takes <code>value</code> as an input string and builds
     * <code>all</code> list in a way that each element ('partition') of the
     * list contains not more than <code>lines</code> of text.
     * <code>delim</code> is used as an line separator. If
     * <code>lines <= 0</code> the <code>all</code> list will contain only
     * single element consist of all previously added values.
     * 
     * @param all
     *            the list of text partitions
     * @param value
     *            the input string
     * @param lines
     *            the maximum number of lines in each partition
     * @param delim
     *            the line delimiter
     * @return the latest added string. Basically it always equals to
     *         <code>value</value>
     */
    public static String splitLines(List<TypedKeyValue<String, Integer>> all,
            String value, int lines, String delim)
    {
        if (all == null || value == null || delim == null)
            return value;
        
        if (all.size() == 0)
        {
            all.add(new TypedKeyValue<String, Integer>("", 0));
        }
        
        TypedKeyValue<String, Integer> current = null;
        
        if (lines <= 0)
        {
            current = all.get(all.size() - 1);
            
            if (Utils.isNothing(current.getKey()))
                current.setKey(value);
            else
                current.setKey(current.getKey() + delim + value);
            
            all.set(all.size() - 1, current);
            
            return value;
        }
        
        current = all.get(all.size() - 1);
        
        String text = current.getKey();
        
        int valueLines = value.split(delim, -1).length;
        
        if (Utils.isNothing(text))
        {
            current.setKey(value);
            current.setValue(valueLines);
        }
        else
        {
            int curLines = current.getValue();
            
            if (curLines + valueLines > lines)
            {
                current = new TypedKeyValue<String, Integer>(value, valueLines);
                
                all.add(current);
            }
            else
            {
                current.setKey(text + delim + value);
                current.setValue(curLines + valueLines);
            }
        }
        
        return all.size() > 0 ? all.get(all.size() - 1).getKey() : "";
    }
    
    /**
     * Converts string to Boolean. If there is not match or input string is null
     * returns <code>defaultValue</code>.
     * 
     * <p>
     * Possible input values (case insensitive): : Y,N,YES,NO,TRUE,FALSE,1,0
     * 
     * @param value
     *            the input string
     * @param defaultValue
     *            the default value
     * @return the boolean
     */
    public static Boolean str2Boolean(String value, Boolean defaultValue)
    {
        if (isNothing(value))
            return defaultValue;
        
        if ("YES".equalsIgnoreCase(value))
            return Boolean.TRUE;
        else if ("NO".equalsIgnoreCase(value))
            return Boolean.FALSE;
        else if ("TRUE".equalsIgnoreCase(value))
            return Boolean.TRUE;
        else if ("FALSE".equalsIgnoreCase(value))
            return Boolean.FALSE;
        else if ("Y".equalsIgnoreCase(value))
            return Boolean.TRUE;
        else if ("N".equalsIgnoreCase(value))
            return Boolean.FALSE;
        else if ("1".equalsIgnoreCase(value))
            return Boolean.TRUE;
        else if ("0".equalsIgnoreCase(value))
            return Boolean.FALSE;
        else
            return defaultValue;
    }
    
    /**
     * Converts string to byte. If there is an error or input value is null
     * returns <code>def</code>.
     * 
     * @param str
     *            the input string
     * @param def
     *            the default value
     * @return the byte
     */
    public static byte str2Byte(String str, byte def)
    {
        if (str == null)
            return def;
        
        try
        {
            return new BigDecimal(str).byteValue();
        }
        catch (NumberFormatException ex)
        {
            return def;
        }
    }
    
    /**
     * Converts string to date using <code>format</code> string. If there is an
     * error or input value is null returns <code>def</code>.
     * 
     * @param str
     *            the input string
     * @param def
     *            the default value
     * @param format
     *            The format being applied to the formatter. Specifications can
     *            be found in {@link java.text.SimpleDateFormat}.
     * @return the Date
     */
    public static Date str2Date(String str, Date def, String format)
    {
        if (str == null)
            return def;
        
        try
        {
            DateFormat formatter = new SimpleDateFormat(format);
            return formatter.parse(str);
        }
        catch (ParseException ex)
        {
            return def;
        }
    }
    
    /**
     * Converts string to date using array of formats. Function will apply
     * formats from the array one by one until there is an
     * <code>ParseException</code>. Additionally it will double check result by
     * comparing input string with the
     * <code>Utils.date2Str(result, format)</code>. If there is no match returns
     * null.
     * 
     * @param input
     *            the input
     * @param formats
     *            the formats
     * @return the Date
     */
    public static Date str2Date(String input, String[] formats)
    {
        if (isNothing(input))
            return null;
        
        SimpleDateFormat formater = null;
        
        for (String format : formats)
            try
            {
                formater = new SimpleDateFormat(format);
                
                Date date = formater.parse(input);
                
                if (date == null || !date2Str(date, format).equals(input))
                    continue;
                
                return date;
            }
            catch (ParseException ex)
            {
                continue;
            }
        
        return null;
    }
    
    /**
     * Converts string to int. If there is an error or input value is null
     * returns <code>def</code>.
     * 
     * @param str
     *            the input string
     * @param def
     *            the default value
     * @return the int
     */
    public static int str2Int(String str, int def)
    {
        if (str == null)
            return def;
        
        try
        {
            return new BigDecimal(str).intValue();
        }
        catch (NumberFormatException ex)
        {
            return def;
        }
    }
    
    /**
     * Converts string to Integer. If there is an error or input value is null
     * returns <code>def</code>.
     * 
     * @param str
     *            the input string
     * @param def
     *            the default value
     * @return the Integer
     */
    public static Integer str2Integer(String str, Integer def)
    {
        if (str == null)
            return def;
        
        try
        {
            return new BigDecimal(str).intValue();
        }
        catch (NumberFormatException ex)
        {
            return def;
        }
    }
    
    /**
     * Converts string to long. If there is an error or input value is null
     * returns <code>def</code>.
     * 
     * @param str
     *            the input string
     * @param def
     *            the default value
     * @return the long
     */
    public static long str2Long(String str, long def)
    {
        if (str == null)
            return def;
        
        try
        {
            return new BigDecimal(str).longValue();
        }
        catch (NumberFormatException ex)
        {
            return def;
        }
    }
    
    /**
     * Converts string to Long. If there is an error or input value is null
     * returns <code>def</code>.
     * 
     * @param str
     *            the input string
     * @param def
     *            the default value
     * @return the Long
     */
    public static Long str2Long(String str, Long def)
    {
        if (str == null)
            return def;
        
        try
        {
            return new BigDecimal(str).longValue();
        }
        catch (NumberFormatException ex)
        {
            return def;
        }
    }
    
    /**
     * Converts string to the 'name' by removing whitespaces and other special
     * characters (such as '_').
     * 
     * <p>
     * Utils.str2Name("test") -> "test"
     * <p>
     * Utils.str2Name("test.xml") -> "testxml"
     * <p>
     * Utils.str2Name("  te   st  ") -> "test"
     * <p>
     * Utils.str2Name("-te___st---") -> "test"
     * <p>
     * Utils.str2Name("test/123/abc") -> "test123abc"
     * 
     * @param value
     *            the input string
     * @return the string
     */
    public static String str2Name(String value)
    {
        if (isNothing(value))
            return null;
        
        return value.replaceAll("\\W", "").replaceAll("_", "");
    }
    
    /**
     * Converts string to Number. If there is an error or input value is null
     * returns <code>def</code>.
     * 
     * <p>
     * Utils.str2Number("11.23", null) -> 11.23
     * <p>
     * Utils.str2Number("11", null) -> 11
     * 
     * @param value
     *            the value
     * @param defaultValue
     *            the default value
     * @return the Number
     */
    public static Number str2Number(String value, Number defaultValue)
    {
        if (isNothing(value))
            return defaultValue;
        
        try
        {
            BigDecimal big = new BigDecimal(value);
            
            if (big.scale() <= 0)
            {
                Long result = big.longValue();
                
                if (Math.abs(result) > Integer.MAX_VALUE)
                    return result;
                else
                    return result.intValue();
            }
            else
            {
                Float result = big.floatValue();
                
                if (result.isNaN() || result.isInfinite())
                {
                    return big.doubleValue();
                }
                
                return result.floatValue();
            }
        }
        catch (NumberFormatException ex)
        {
            return defaultValue;
        }
    }
    
    /**
     * Parses string like name=value;name=value and converts it to
     * name='value';name='value';
     * 
     * <p>
     * Utils.str2PropsStr("abc=aaa;xyz=123;fff=777;xyz=mmm", ";", "'") ->
     * "abc='aaa';xyz='123';fff='777';xyz='mmm';"
     * 
     * @param value
     *            the input string
     * @param delim
     *            the delimeter
     * @param quote
     *            the character used a quote
     * @return the string
     */
    public static String str2PropsStr(String value, String delim, String quote)
    {
        if (isNothing(value))
            return "";
        
        String ret = "";
        
        LinkedHashMap<String, String> props = getPropertiesMap(value);
        
        for (String name : props.keySet())
        {
            ret = ret + name.toString().trim() + "=" + quote
                    + Utils.makeString(props.get(name)).trim() + quote + delim;
        }
        
        return ret.trim();
    }
    
    /**
     * Converts string with special chars used by Java regular expression engine
     * to the string where these chars precede by escapes character.
     * 
     * <p>
     * Utils.str2Regexp("567") -> "567"
     * <p>
     * Utils.str2Regexp("\\s+str") -> "\\s\\+str"
     * 
     * @param value
     *            input string
     * @return the string
     */
    public static String str2Regexp(String value)
    {
        if (value == null)
            return null;
        
        return value.replaceAll("([^a-zA-z0-9])", "\\\\$1");
    }
    
    /**
     * If fiven <code>str</code> is not null returns <code>str</code>, otherwise returns <code>def</code>.
     *
     * @param str the string
     * @param def the default value
     * @return the string
     */
    public static String str2Str(String str, String def)
    {
        return str != null ? str : def;
    }
    
    /**
     * Trims string by removing all whitespaces.
     * 
     * <p>
     * Utils.trim("  this     is a string  ") -> "thisisastring"
     * 
     * @param value
     *            the input string
     * @return the string
     */
    public static String trim(String value)
    {
        if (value == null)
            return null;
        
        return value.trim().replaceAll("\\s+", "");
    }
    
    /**
     * Creates a string by concatenatig <code>value</code> and <code>add</code>
     * so the new string.length is <= <code>length</code>.
     * 
     * <p>
     * Utils.trim("abcd123", 4, null) -> "abcd" Utils.trim("abcd123", 4, "xyz")
     * -> "axyz"
     * 
     * @param value
     *            the input string
     * @param length
     *            the length of the result string
     * @param add
     *            the string to add
     * @return the string
     */
    public static String trim(String value, int length, String add)
    {
        if (value == null || value.length() <= length || length <= 0)
            return value;
        
        return value.substring(0, length - (add != null ? add.length() : 0))
                + (add != null ? add : "");
    }
    
    /**
     * Trims left part of the string so it length is <= <code>size</code>.
     * 
     * <p>
     * Utils.trimLeft("1234567", 3) -> "567"
     * 
     * @param value
     *            the input string
     * @param size
     *            the size of the new string
     * @return the string
     */
    public static String trimLeft(String value, int size)
    {
        if (value == null)
            return null;
        
        if (value.length() > size)
            value = value.substring(value.length() - size);
        
        return value;
    }
    
    /**
     * Truncates input string by removing everything after (and including)
     * <code>toTrunc</code>.
     * 
     * <p>
     * Utils.truncEnd("abc\n", "\n") -> "abc" Utils.truncEnd("xyz", "m") - >
     * "xyz"
     * 
     * @param s
     *            the input string
     * @param toTrunc
     *            substing to truncate
     * @return the string
     */
    public static String truncEnd(String s, String toTrunc)
    {
        int pos = s.lastIndexOf(toTrunc);
        if (pos == s.length() - toTrunc.length())
            s = s.substring(0, s.length() - toTrunc.length());
        
        return s;
    }
    
    /**
     * Convert input value to the object of the given type name.
     *
     * @param typeName the type name
     * @param value the input value
     * @return the object
     */
    public static Object value2Type(String typeName, Object value)
    {
        if (value == null)
            return null;
        
        if (STRING.equalsIgnoreCase(typeName))
        {
            if (value instanceof String)
                return value;
            else
                return value.toString();
        }
        else if (INTEGER.equalsIgnoreCase(typeName))
        {
            if (value instanceof Number)
                return ((Number)value).intValue();
            else
                return Utils.str2Integer(value.toString(), null);
        }
        else if (LONG.equalsIgnoreCase(typeName))
        {
            if (value instanceof Number)
                return ((Number)value).longValue();
            else
                return Utils.str2Long(value.toString(), null);
            
        }
        else if (NUMBER.equalsIgnoreCase(typeName))
        {
            if (value instanceof Number)
                return value;
            else
                return Utils.str2Number(value.toString(), null);
            
        }
        else if (DATE.equalsIgnoreCase(typeName))
        {
            if (value instanceof Date)
            {
                Date date = setTimeToMidnight((Date)value);
                
                return date;
            }
            else
            {
                try
                {
                    return DateUtil.parse(value.toString());
                }
                catch (ParseException ex)
                {
                    return null;
                }
                
            }
        }
        else if (TIME.equalsIgnoreCase(typeName))
        {
            if (value instanceof Date)
            {
                Date date = setDateToBeginningOfTime((Date)value);
                
                return date;
            }
            else
            {
                try
                {
                    return DateUtil.parse(value.toString());
                }
                catch (ParseException ex)
                {
                    return null;
                }
                
            }
        }
        else if (TIMESTAMP.equalsIgnoreCase(typeName))
        {
            if (value instanceof Date)
            {
                return value;
            }
            else
            {
                try
                {
                    return DateUtil.parse(value.toString());
                }
                catch (ParseException ex)
                {
                    return null;
                }
                
            }
        }
        else if (Utils.BOOLEAN.equalsIgnoreCase(typeName))
        {
            if (value instanceof Boolean)
                return value;
            else
                return Utils.str2Boolean(value.toString(), false);
        }
        else
        {
            return value;
        }
    }
    
}
