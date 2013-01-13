/*
 * Variable.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.util.Utils;

/**
 * This class defines variable. Used by etl framework, sql developer plug in and
 * other modules.
 * 
 * @see com.toolsverse.etl.common.Function
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Variable implements Serializable
{
    
    /** The DEFAULT LANGUAGE. */
    public static final String DEFAULT_LANG = "sql";
    
    /** The EXECUTE AS CONFIGURED scope. */
    public static final int EXECUTE_AS_CONFIGURED = 0;
    
    /** The EXECUTE BEFORE scope. */
    public static final int EXECUTE_BEFORE = 2;
    
    /** The EXECUTE AFTER scope. */
    public static final int EXECUTE_AFTER = 4;
    
    /** The EXECUTE AT RUNTIME scope. */
    public static final int EXECUTE_RUNTIME = 8;
    
    /** The EXECUTE_BEFORE type. */
    public static final String EXECUTE_BEFORE_STR = "before";
    
    /** The EXECUTE_AFTER type. */
    public static final String EXECUTE_AFTER_STR = "after";
    
    /** The EXECUTE AT RUNTIME type. */
    public static final String EXECUTE_RUNTIME_STR = "runtime";
    
    /** The TYPE NAME ATTRIBUTE. */
    public static final String NATIVE_TYPE_ATTR = "nativetype";
    
    /** The SQL TYPE ATTRIBUTE. */
    public static final String SQL_TYPE_ATTR = "sqltype";
    
    /** The execution scopes. */
    public static final Map<String, Integer> EXECUTE_SCOPE = new HashMap<String, Integer>();
    static
    {
        EXECUTE_SCOPE.put(EXECUTE_BEFORE_STR, EXECUTE_BEFORE);
        EXECUTE_SCOPE.put(EXECUTE_AFTER_STR, EXECUTE_AFTER);
        EXECUTE_SCOPE.put(EXECUTE_RUNTIME_STR, EXECUTE_RUNTIME);
    }
    
    /** The name. */
    private String _name;
    
    /** The value. */
    private String _value;
    
    /** The table name. */
    private String _tableName;
    
    /** The type. */
    private String _type;
    
    /** The attributes. */
    private Map<String, String> _attrs;
    
    /** The function class name. */
    private String _functionClassName;
    
    /** The field name. */
    private String _fieldName;
    
    /** The linked cache provider. */
    private CacheProvider<String, Object> _linkedCacheProvider;
    
    /** The linked variable name. */
    private String _linkedVarName;
    
    /** The function. */
    private String _function;
    
    /** The code. */
    private String _code;
    
    /** The language. */
    private String _lang;
    
    /** The label. */
    private String _label;
    
    /** The parameters. */
    private String _param;
    
    /** The include flag. */
    private boolean _include;
    
    /** The global flag. */
    private boolean _global;
    
    /** The tolerate flag. */
    private boolean _tolerate;
    
    /** The scope. */
    private int _scope;
    
    /** The object. */
    private Object _object;
    
    /** The _declare. */
    private String _declare;
    
    /**
     * Instantiates a new variable.
     */
    public Variable()
    {
        _name = null;
        _value = null;
        _tableName = null;
        _type = null;
        _attrs = null;
        _declare = null;
        
        _functionClassName = null;
        _fieldName = null;
        _linkedCacheProvider = null;
        _linkedVarName = null;
        _function = null;
        _code = null;
        _label = null;
        _param = null;
        _include = true;
        _global = false;
        _tolerate = false;
        _scope = EXECUTE_AS_CONFIGURED;
        _object = null;
        
        _lang = DEFAULT_LANG;
    }
    
    /**
     * Adds the scope. The scope is bitmask so multiple scopes can be set at the
     * same time.
     * 
     * @param value
     *            the scope to add
     */
    public void addScope(int value)
    {
        _scope = _scope | value;
    }
    
    /**
     * Removes attribute.
     *
     * @param name the name
     */
    public void clearAttr(String name)
    {
        if (_attrs != null)
            _attrs.remove(name);
    }
    
    /**
     * Gets the attributes.
     * 
     * @return the attributes
     */
    public Map<String, String> getAttrs()
    {
        return _attrs;
    }
    
    /**
     * Gets the attribute value by name.
     * 
     * @param name
     *            the attribute name.
     * @return the value
     */
    public String getAttrValue(String name)
    {
        return _attrs != null ? _attrs.get(name) : null;
    }
    
    /**
     * Gets the code. Can be sql, javascript or any other supported language.
     * Used by functions to calculate variable value.
     * 
     * @return the code
     * @see com.toolsverse.etl.common.Function
     */
    public String getCode()
    {
        return _code;
    }
    
    /**
     * Gets the declare code.
     *
     * @return the declare code
     */
    public String getDeclare()
    {
        if (Utils.isNothing(_declare))
            return "";
        
        return _declare + " ";
    }
    
    /**
     * Gets the name used by function. Can be different from variable name and
     * is either field name (if set) or variable name.
     * 
     * @return the evaluation name
     */
    public String getEvalName()
    {
        if (!Utils.isNothing(_fieldName))
            return _fieldName;
        else
            return getName();
    }
    
    /**
     * Gets the field name.
     * 
     * @return the field name
     */
    public String getFieldName()
    {
        return _fieldName;
    }
    
    /**
     * Gets the function name.
     * 
     * @return the function name
     */
    public String getFunction()
    {
        return _function;
    }
    
    /**
     * Gets the function class name.
     * 
     * @return the function class name
     */
    public String getFunctionClassName()
    {
        return _functionClassName;
    }
    
    /**
     * Gets the label. Most commonly used as a field name for the sql statement.
     * 
     * @return the label
     */
    public String getLabel()
    {
        return _label;
    }
    
    /**
     * Gets the code language. The default is sql.
     * 
     * @return the the code language
     */
    public String getLang()
    {
        return _lang;
    }
    
    /**
     * Gets the linked cache provider.
     * 
     * @return the linked cache provider
     */
    public CacheProvider<String, Object> getLinkedCacheProvider()
    {
        return _linkedCacheProvider;
    }
    
    /**
     * Gets the linked variable name.
     * 
     * @return the linked variable name
     */
    public String getLinkedVarName()
    {
        return _linkedVarName;
    }
    
    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String getName()
    {
        return _name;
    }
    
    /**
     * Gets the object. Any object can be associated with the variable.
     * 
     * @return the object
     */
    public Object getObject()
    {
        return _object;
    }
    
    /**
     * Gets the parameters.
     * 
     * @return the parameters
     */
    public String getParam()
    {
        return _param;
    }
    
    /**
     * Gets the scope.
     * 
     * @return the scope
     */
    public int getScope()
    {
        return _scope;
    }
    
    /**
     * Gets the table name.
     * 
     * @return the table name
     */
    public String getTableName()
    {
        return _tableName;
    }
    
    /**
     * Gets the type. Used to define sql data type for the variable declaration.
     * 
     * @return the type
     */
    public String getType()
    {
        return _type;
    }
    
    /**
     * Gets the value.
     * 
     * @return the value
     */
    public String getValue()
    {
        return _value;
    }
    
    /**
     * Checks if variable need to be declared.
     *
     * @return true, if variable need to be declared
     */
    public boolean isDeclare()
    {
        return Utils.isNothing(_declare) || Utils.str2Boolean(_declare, true);
    }
    
    /**
     * Checks if variable is global.
     * 
     * @return true, if is global
     */
    public boolean isGlobal()
    {
        return _global;
    }
    
    /**
     * Checks if "is include" flag set. If it is false the field will be
     * excluded from various etl actions such as insert into, update, ect. The
     * default value is true.
     * 
     * @return true, if "is include" flag set
     */
    public boolean isInclude()
    {
        return _include;
    }
    
    /**
     * Checks if given scope is set.
     * 
     * @param value
     *            the scope
     * @return true, if given scope is set
     */
    public boolean isScopeSet(int value)
    {
        return ((_scope & value) == value);
    }
    
    /**
     * Checks if ignore any exception during function execution flag is set.
     * 
     * @return true, if ignore any exception during function execution flag is
     *         set
     */
    public boolean isTolerate()
    {
        return _tolerate;
    }
    
    /**
     * Parses the scope string. Returns bitmask. Expected format:
     * scope1|scope2|etc. Example: before|after
     * 
     * @param scope
     *            the scope string
     * @return the scope bitmask
     */
    public int parseScope(String scope)
    {
        if (Utils.isNothing(scope))
        {
            _scope = EXECUTE_AS_CONFIGURED;
            return _scope;
        }
        
        String[] scopes = scope.split("\\|", -1);
        
        for (int i = 0; i < scopes.length; i++)
        {
            Integer value = EXECUTE_SCOPE.get(scopes[i].toLowerCase().trim());
            
            if (value != null)
                addScope(value);
        }
        
        return _scope;
        
    }
    
    /**
     * Sets the attributes.
     * 
     * @param value
     *            the new attributes
     */
    public void setAttrs(Map<String, String> value)
    {
        _attrs = value;
    }
    
    /**
     * Sets the attribute value.
     *
     * @param name the name
     * @param value the value
     */
    public void setAttrValue(String name, String value)
    {
        if (_attrs == null)
            _attrs = new HashMap<String, String>();
        
        _attrs.put(name, value);
    }
    
    /**
     * Sets the code.
     * 
     * @param value
     *            the new code
     */
    public void setCode(String value)
    {
        _code = value;
    }
    
    /**
     * Sets the declare code.
     *
     * @param value the new declare code
     */
    public void setDeclare(String value)
    {
        _declare = value;
    }
    
    /**
     * Sets the field name.
     * 
     * @param value
     *            the new field name
     */
    public void setFieldName(String value)
    {
        _fieldName = value;
    }
    
    /**
     * Sets the function.
     * 
     * @param value
     *            the new function
     */
    public void setFunction(String value)
    {
        _function = value;
    }
    
    /**
     * Sets the function class name.
     * 
     * @param value
     *            the new function class name
     */
    public void setFunctionClassName(String value)
    {
        _functionClassName = value;
    }
    
    /**
     * Sets the "is global" flag.
     * 
     * @param value
     *            the new value for "is global" flag
     */
    public void setIsGlobal(boolean value)
    {
        _global = value;
    }
    
    /**
     * Sets "is include" flag.
     * 
     * @param value
     *            the new value for "is include" flag
     */
    public void setIsInclude(boolean value)
    {
        _include = value;
    }
    
    /**
     * Sets the "tolerate" flag.
     * 
     * @param value
     *            the new value for "tolerate" flag
     */
    public void setIsTolerate(boolean value)
    {
        _tolerate = value;
    }
    
    /**
     * Sets the label.
     * 
     * @param value
     *            the new label
     */
    public void setLabel(String value)
    {
        _label = value;
    }
    
    /**
     * Sets the code language. For example sql, javascript, etc.
     * 
     * @param value
     *            the new code language
     */
    public void setLang(String value)
    {
        _lang = value;
    }
    
    /**
     * Sets the linked cache provider.
     * 
     * @param value
     *            the new linked cache provider
     */
    public void setLinkedCacheProvider(CacheProvider<String, Object> value)
    {
        _linkedCacheProvider = value;
    }
    
    /**
     * Sets the linked variable name.
     * 
     * @param value
     *            the new linked variable name
     */
    public void setLinkedVarName(String value)
    {
        _linkedVarName = value;
    }
    
    /**
     * Sets the name.
     * 
     * @param value
     *            the new name
     */
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the object.
     * 
     * @param value
     *            the new object
     */
    public void setObject(Object value)
    {
        _object = value;
    }
    
    /**
     * Sets the parameters.
     * 
     * @param value
     *            the new parameters
     */
    public void setParam(String value)
    {
        _param = value;
    }
    
    /**
     * Sets the table name.
     * 
     * @param value
     *            the new table name
     */
    public void setTableName(String value)
    {
        _tableName = value;
    }
    
    /**
     * Sets the type.
     * 
     * @param value
     *            the new type
     */
    public void setType(String value)
    {
        _type = value;
    }
    
    /**
     * Sets the value.
     * 
     * @param value
     *            the new value
     */
    public void setValue(String value)
    {
        _value = value;
    }
    
}
