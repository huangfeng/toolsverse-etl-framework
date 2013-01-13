/*
 * GenericSqlParser.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.parser;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.Element;

import org.fife.ui.rsyntaxtextarea.RSyntaxDocument;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.Token;
import org.fife.ui.rsyntaxtextarea.parser.AbstractParser;
import org.fife.ui.rsyntaxtextarea.parser.DefaultParseResult;
import org.fife.ui.rsyntaxtextarea.parser.ParseResult;

import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.DateUtil;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the SqlParser interface. Uses RSyntaxDocument
 * as an actual sql parser.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class GenericSqlParser implements SqlParser
{
    /**
     * The Class ParamsParser.
     */
    public class ParamsParser extends AbstractParser
    {
        
        /** The params. */
        private Map<String, Object> _params = null;
        
        /** The param map. */
        private Map<String, List<Integer>> _paramMap = null;
        
        /** The output params. */
        private List<String> _outputParams = null;
        
        /** The driver. */
        private Driver _driver = null;
        
        /** The sql. */
        private String _sql = null;
        
        /** The pattern. */
        private Pattern _pattern = null;
        
        /**
         * Instantiates a new ParamsParser.
         * 
         * @param sql
         *            the sql
         * @param params
         *            the bind variables.
         * @param paramMap
         *            the map containing bind variable name name as key and a
         *            list of parameter indexes as a value.
         *            <p>
         *            For example:
         *            <p>
         * 
         *            <pre>
         * select * from abc
         * where xyz = :xyz and mmm = :xyz
         * </pre>
         *            <p>
         *            the map will contain "xyz" as a key and a list {1,2} as a
         *            value.
         * @param outputParams
         *            the output params
         * @param driver
         *            the driver * @param pattern the pattern
         * @param pattern
         *            the pattern
         */
        public ParamsParser(String sql, Map<String, Object> params,
                Map<String, List<Integer>> paramMap, List<String> outputParams,
                Driver driver, Pattern pattern)
        {
            super();
            
            _sql = sql;
            _params = params;
            _paramMap = paramMap;
            _outputParams = outputParams;
            _driver = driver;
            _pattern = pattern;
        }
        
        /**
         * Gets the sql.
         * 
         * @return the sql
         */
        public String getSql()
        {
            return _sql;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @seeorg.fife.ui.rsyntaxtextarea.parser.Parser#parse(org.fife.ui.
         * rsyntaxtextarea.RSyntaxDocument, java.lang.String)
         */
        public ParseResult parse(RSyntaxDocument doc, String style)
        {
            try
            {
                _sql = doc.getText(0, doc.getLength());
                
            }
            catch (Exception ex)
            {
                Logger.log(Logger.INFO, this,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
                _sql = null;
            }
            
            DefaultParseResult result = new DefaultParseResult(this);
            
            Element root = doc.getDefaultRootElement();
            int lineCount = root.getElementCount();
            
            result.clearNotices();
            result.setParsedLines(0, lineCount - 1);
            
            int index = 1;
            
            for (int line = 0; line < lineCount; line++)
            {
                Token t = doc.getTokenListForLine(line);
                int offs = -1;
                String name = null;
                
                while (t != null && t.isPaintable())
                {
                    if (t.type != Token.COMMENT_EOL
                            && t.type != Token.COMMENT_MULTILINE
                            && t.type != Token.COMMENT_DOCUMENTATION
                            && t.type != Token.LITERAL_CHAR
                            && t.type != Token.LITERAL_STRING_DOUBLE_QUOTE
                            && t.type != Token.LITERAL_BACKQUOTE)
                    {
                        
                        offs = t.offset;
                        name = t.getLexeme();
                        
                        Matcher m = _pattern.matcher(name);
                        while (m.find())
                        {
                            name = name.substring(m.start() + 1, m.end());
                            
                            if (!Utils.isNothing(name))
                            {
                                offs += m.start();
                                
                                char[] fill = new char[m.end() - m.start() - 1];
                                
                                Arrays.fill(fill, ' ');
                                
                                String jdcbParam = "?" + String.valueOf(fill);
                                
                                _sql = _sql.substring(0, offs)
                                        + jdcbParam
                                        + _sql.substring(offs
                                                + jdcbParam.length());
                                
                                Variable var = getVariable(name, _driver);
                                
                                if (_outputParams != null && var != null)
                                    _outputParams.add(name);
                                
                                if (_params != null
                                        && (var == null || _outputParams == null)
                                        && !_params.containsKey(name))
                                    _params.put(name, null);
                                
                                if (_paramMap != null)
                                {
                                    
                                    List<Integer> indexList = _paramMap
                                            .get(name);
                                    if (indexList == null)
                                    {
                                        indexList = new ArrayList<Integer>();
                                        _paramMap.put(name, indexList);
                                    }
                                    indexList.add(new Integer(index));
                                }
                                
                                index++;
                            }
                        }
                    }
                    
                    t = t.getNextToken();
                }
            }
            
            return result;
        }
    }
    
    /**
     * The Class SplitParser.
     */
    public class SplitParser extends AbstractParser
    {
        
        /** The delimiter. */
        private String _splitter = null;
        
        /** The sql. */
        private String _sql = null;
        
        /** The list of tokens. */
        private final List<String> _list = new ArrayList<String>();
        
        /**
         * Instantiates a new SplitParser.
         * 
         * @param splitter
         *            the delimiter
         */
        public SplitParser(String splitter)
        {
            super();
            
            _splitter = splitter;
        }
        
        /**
         * Gets the array of sql statements.
         * 
         * @return the array of sql statements
         */
        public String[] getStatements()
        {
            if (_list.size() == 0)
            {
                if (!Utils.isNothing(stripComments(_sql)))
                    return new String[] {_sql};
                else
                    return new String[] {""};
            }
            
            String[] statements = new String[_list.size()];
            
            _list.toArray(statements);
            
            return statements;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @seeorg.fife.ui.rsyntaxtextarea.parser.Parser#parse(org.fife.ui.
         * rsyntaxtextarea.RSyntaxDocument, java.lang.String)
         */
        public ParseResult parse(RSyntaxDocument doc, String style)
        {
            try
            {
                _sql = doc.getText(0, doc.getLength());
                
            }
            catch (Exception ex)
            {
                Logger.log(Logger.INFO, this,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
                _sql = null;
            }
            
            DefaultParseResult result = new DefaultParseResult(this);
            
            Element root = doc.getDefaultRootElement();
            int lineCount = root.getElementCount();
            
            result.clearNotices();
            result.setParsedLines(0, lineCount - 1);
            
            int lastOffs = -1;
            
            for (int line = 0; line < lineCount; line++)
            {
                Token t = doc.getTokenListForLine(line);
                int offs = -1;
                String name = null;
                
                while (t != null && t.isPaintable())
                {
                    if (t.type != Token.COMMENT_EOL
                            && t.type != Token.COMMENT_MULTILINE
                            && t.type != Token.COMMENT_DOCUMENTATION
                            && t.type != Token.LITERAL_CHAR
                            && t.type != Token.LITERAL_STRING_DOUBLE_QUOTE
                            && t.type != Token.LITERAL_BACKQUOTE)
                    {
                        
                        offs = t.offset;
                        name = t.getLexeme();
                        
                        if (_splitter.equalsIgnoreCase(name))
                        {
                            _list.add(_sql.substring(lastOffs >= 0 ? lastOffs
                                    : 0, offs));
                            
                            lastOffs = offs + 1;
                        }
                        
                    }
                    
                    t = t.getNextToken();
                }
            }
            
            if (lastOffs >= 0 && lastOffs < _sql.length())
            {
                String lastSt = _sql.substring(lastOffs, _sql.length());
                
                if (!Utils.isNothing(GenericSqlParser.this
                        .stripComments(lastSt)))
                    _list.add(lastSt);
            }
            
            return result;
        }
    }
    
    /**
     * The Class SqlTypeParser.
     */
    public class SqlTypeParser extends AbstractParser
    {
        
        /** The "is callable" flag. */
        private boolean _isCallable = false;
        
        /**
         * Gets the sql.
         * 
         * @return the sql
         */
        public boolean isCallable()
        {
            return _isCallable;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @seeorg.fife.ui.rsyntaxtextarea.parser.Parser#parse(org.fife.ui.
         * rsyntaxtextarea.RSyntaxDocument, java.lang.String)
         */
        public ParseResult parse(RSyntaxDocument doc, String style)
        {
            DefaultParseResult result = new DefaultParseResult(this);
            
            Element root = doc.getDefaultRootElement();
            int lineCount = root.getElementCount();
            
            result.clearNotices();
            result.setParsedLines(0, lineCount - 1);
            
            boolean hasBegin = false;
            boolean hasEnd = false;
            
            for (int line = 0; line < lineCount; line++)
            {
                Token t = doc.getTokenListForLine(line);
                String name = null;
                
                while (t != null && t.isPaintable())
                {
                    if (t.type != Token.COMMENT_EOL
                            && t.type != Token.COMMENT_MULTILINE
                            && t.type != Token.COMMENT_DOCUMENTATION
                            && t.type != Token.LITERAL_CHAR
                            && t.type != Token.LITERAL_STRING_DOUBLE_QUOTE
                            && t.type != Token.LITERAL_BACKQUOTE)
                    {
                        
                        name = t.getLexeme();
                        
                        hasBegin = hasBegin
                                || name.equalsIgnoreCase(BEGIN_TOKEN)
                                || name.equalsIgnoreCase(CREATE_TOKEN);
                        
                        hasEnd = hasEnd
                                || (hasBegin && name
                                        .equalsIgnoreCase(END_TOKEN));
                    }
                    
                    t = t.getNextToken();
                }
            }
            
            _isCallable = hasBegin && hasEnd;
            
            return result;
        }
    }
    
    /**
     * The Class StripParser.
     */
    public class StripParser extends AbstractParser
    {
        /** The sql. */
        private String _statement;
        
        /**
         * Instantiates a new StripParser.
         * 
         */
        public StripParser()
        {
            super();
            
            _statement = "";
        }
        
        /**
         * Gets the sql statement.
         * 
         * @return the sql statement
         */
        public String getStatement()
        {
            return _statement;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @seeorg.fife.ui.rsyntaxtextarea.parser.Parser#parse(org.fife.ui.
         * rsyntaxtextarea.RSyntaxDocument, java.lang.String)
         */
        public ParseResult parse(RSyntaxDocument doc, String style)
        {
            DefaultParseResult result = new DefaultParseResult(this);
            
            Element root = doc.getDefaultRootElement();
            int lineCount = root.getElementCount();
            
            result.clearNotices();
            result.setParsedLines(0, lineCount - 1);
            
            for (int line = 0; line < lineCount; line++)
            {
                Token t = doc.getTokenListForLine(line);
                
                while (t != null && t.isPaintable())
                {
                    if (t.type != Token.COMMENT_EOL
                            && t.type != Token.COMMENT_MULTILINE
                            && t.type != Token.COMMENT_DOCUMENTATION)
                    {
                        
                        _statement = _statement + t.getLexeme();
                        
                    }
                    
                    t = t.getNextToken();
                }
            }
            
            return result;
        }
    }
    
    /** The PATTERN. */
    private static final Pattern PATTERN = Pattern.compile(":([a-zA-Z_0-9]+)");
    
    /** The BEGIN_TOKEN. */
    private static final String BEGIN_TOKEN = "begin";
    
    /** The END_TOKEN. */
    private static final String END_TOKEN = "end";
    
    /** The CREATE_TOKEN. */
    private static final String CREATE_TOKEN = "create";
    
    /**
     * Gets the instance of RSyntaxDocument.
     * 
     * @param sql
     *            the sql
     * @return the instance of the RSyntaxDocument
     */
    private RSyntaxDocument getSqlDocument(String sql)
    {
        RSyntaxDocument doc = new RSyntaxDocument(
                SyntaxConstants.SYNTAX_STYLE_SQL);
        
        try
        {
            doc.insertString(0, sql, null);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.INFO, this, Resource.ERROR_GENERAL.getValue(), ex);
            
            return null;
        }
        
        return doc;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#getVariable(java.lang.String,
     * com.toolsverse.etl.driver.Driver)
     */
    public Variable getVariable(String name, Driver driver)
    {
        Variable var = null;
        
        if (Utils.isNothing(name))
            return null;
        
        String[] vars = name.split("_", -1);
        
        if (vars == null || vars.length < 2)
            return null;
        
        if (!"OUT".equalsIgnoreCase(vars[0]))
            return null;
        
        name = name.substring((vars[0] + "_" + vars[1]).length() + 1);
        
        var = new Variable();
        var.setName(name);
        
        var.setType(String.valueOf(getVarType(vars[1], driver)));
        
        return var;
    }
    
    /**
     * Gets the variable type.
     * 
     * @param type
     *            the type
     * @param driver
     *            the driver
     * @return the variable type
     */
    private int getVarType(String type, Driver driver)
    {
        if ("CURSOR".equalsIgnoreCase(type))
            return driver.getParamType(type);
        
        Integer sqlType = SqlUtils.getTypeByName(type);
        
        if (sqlType == null)
        {
            boolean negative = false;
            
            if (type.toLowerCase().indexOf("minus") == 0)
            {
                type = type.substring(5);
                negative = true;
            }
            
            sqlType = Utils.str2Integer(type, null);
            
            return sqlType != null ? sqlType.intValue() * (negative ? -1 : 1)
                    : Types.OTHER;
        }
        
        return sqlType;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#isCallable(java.lang.String)
     */
    public boolean isCallable(String sql)
    {
        RSyntaxDocument doc = getSqlDocument(sql);
        
        if (doc == null)
            return false;
        
        SqlTypeParser parser = new SqlTypeParser();
        
        parser.parse(doc, SyntaxConstants.SYNTAX_STYLE_SQL);
        
        return parser.isCallable();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#parseParams(java.lang.String,
     * java.util.Map, java.util.Map, java.util.List,
     * com.toolsverse.etl.driver.Driver)
     */
    public String parseParams(String sql, Map<String, Object> params,
            Map<String, List<Integer>> paramMap, List<String> outputParams,
            Driver driver)
    {
        if (Utils.isNothing(sql))
            return sql;
        
        RSyntaxDocument doc = getSqlDocument(sql);
        
        if (doc == null)
            return sql;
        
        ParamsParser parser = new ParamsParser(sql, params, paramMap,
                outputParams, driver, PATTERN);
        
        parser.parse(doc, SyntaxConstants.SYNTAX_STYLE_SQL);
        
        return parser.getSql();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#removeChars(java.lang.String)
     */
    public String removeChars(String sql)
    {
        if (sql == null)
            return sql;
        
        return Utils.findAndReplace(sql, "\r", "", false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#setBindVariables(java.sql.
     * PreparedStatement, java.util.Map, java.util.Map, java.util.List,
     * com.toolsverse.etl.driver.Driver)
     */
    public void setBindVariables(PreparedStatement st,
            Map<String, Object> properties,
            Map<String, List<Integer>> paramMap, List<String> outpurParams,
            Driver driver)
        throws Exception
    {
        if (paramMap == null || paramMap.size() == 0)
            return;
        
        if (properties != null || properties.size() > 0)
        {
            
            for (String name : properties.keySet())
            {
                Object value = properties.get(name);
                
                List<Integer> indexes = paramMap.get(name);
                
                if (indexes == null || indexes.size() == 0)
                    continue;
                
                for (int i = 0; i < indexes.size(); i++)
                {
                    int index = indexes.get(i);
                    
                    if (!Utils.isNothing(value))
                    {
                        if (value instanceof String)
                        {
                            
                            try
                            {
                                Date date = DateUtil.parse(value.toString());
                                
                                if (Utils.isDateOnly(date, null))
                                {
                                    st.setDate(index,
                                            new java.sql.Date(date.getTime()));
                                    
                                }
                                else if (Utils.isTimeOnly(date, null))
                                {
                                    st.setTime(index,
                                            new java.sql.Time(date.getTime()));
                                }
                                else
                                {
                                    st.setTimestamp(
                                            index,
                                            new java.sql.Timestamp(date
                                                    .getTime()));
                                }
                            }
                            catch (ParseException ex)
                            {
                                st.setObject(index, value);
                            }
                        }
                        else
                            setParamValue(st, name, index, value);
                    }
                    else
                        st.setNull(index, java.sql.Types.VARCHAR);
                }
            }
        }
        
        if (outpurParams == null || outpurParams.size() == 0
                || !(st instanceof CallableStatement))
            return;
        
        CallableStatement cst = (CallableStatement)st;
        
        for (int i = 0; i < outpurParams.size(); i++)
        {
            String name = outpurParams.get(i);
            
            Variable var = getVariable(name, driver);
            
            if (var == null)
                continue;
            
            List<Integer> indexes = paramMap.get(name);
            
            if (indexes == null || indexes.size() == 0)
                continue;
            
            for (int j = 0; j < indexes.size(); j++)
            {
                int index = indexes.get(j);
                
                cst.registerOutParameter(index, Integer.parseInt(var.getType()));
            }
            
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.parser.SqlParser#setBindVariables(java.lang.String,
     * java.util.Map)
     */
    public String setBindVariables(String sql, Map<String, Object> properties)
    {
        if (properties != null || properties.size() > 0)
        {
            for (String name : properties.keySet())
            {
                sql = Utils.findAndReplace(sql, ":" + name, properties
                        .get(name) != null ? properties.get(name).toString()
                        : null, true);
            }
        }
        
        return sql;
    }
    
    /**
     * Sets the parameter value.
     *
     * @param st the prepared statement
     * @param name the name
     * @param index the index
     * @param value the value
     * @throws SQLException the sQL exception
     */
    private void setParamValue(PreparedStatement st, String name, int index,
            Object value)
        throws SQLException
    {
        String[] tokens = name.split("_");
        
        if (tokens.length == 1)
            st.setObject(index, value);
        else
        {
            Object newValue = Utils.value2Type(tokens[0], value);
            
            if (newValue instanceof Date)
            {
                
                if (Utils.isDateOnly((Date)newValue, null))
                {
                    st.setDate(index,
                            new java.sql.Date(((Date)newValue).getTime()));
                    
                }
                else if (Utils.isTimeOnly((Date)newValue, null))
                {
                    st.setTime(index,
                            new java.sql.Time(((Date)newValue).getTime()));
                }
                else
                {
                    st.setTimestamp(index, new java.sql.Timestamp(
                            ((Date)newValue).getTime()));
                }
            }
            else
                st.setObject(index, newValue);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#split(java.lang.String)
     */
    public String[] split(String sql)
    {
        return split(sql, ";");
    }
    
    /**
     * Splits the sql using given delimiter.
     * 
     * @param sql
     *            the sql
     * @param delimiter
     *            the delimiter
     * @return the string[]
     */
    private String[] split(String sql, String delimiter)
    {
        if (sql == null)
            return null;
        
        if (Utils.isNothing(delimiter))
            return new String[] {sql};
        
        RSyntaxDocument doc = getSqlDocument(sql);
        
        if (doc == null)
            return sql.split(delimiter);
        
        SplitParser parser = new SplitParser(delimiter);
        
        parser.parse(doc, SyntaxConstants.SYNTAX_STYLE_SQL);
        
        return parser.getStatements();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.parser.SqlParser#stripComments(java.lang.String)
     */
    public String stripComments(String sql)
    {
        RSyntaxDocument doc = getSqlDocument(sql);
        
        if (doc == null)
            return sql;
        
        StripParser parser = new StripParser();
        
        parser.parse(doc, SyntaxConstants.SYNTAX_STYLE_SQL);
        
        return parser.getStatement();
        
    }
}