/*
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;

import com.toolsverse.etl.sql.util.SqlUtils;

/**
 * This class defines a database field. 
 * 
 * @see com.toolsverse.etl.common.DataSet
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class FieldDef implements Serializable
{
    
    /** The name. */
    private String _name;
    
    /** The sql data type. */
    private int _sqlDataType;
    
    /** The native data type. */
    private String _nativeDataType;
    
    /** The scale. */
    private int _scale;
    
    /** The precision. */
    private int _precision;
    
    /** The data size. */
    private String _dataSize;
    
    /** The field size. */
    private int _fieldSize;
    
    /** The is nullable flag. */
    private boolean _isNullable;
    
    /** The has params flag. */
    private boolean _hasParams;
    
    /** The best match field. */
    private FieldDef _bestMatch;
    
    /** The encode flag. */
    private boolean _encode;
    
    /** The visible flag. */
    private boolean _visible;
    
    /** The index. */
    private int _index;
    
    /** The is auto increment field. */
    private boolean _isAutoIncrement;
    
    /** number of versions of the the same field */
    private int _versions;
    
    private boolean _toDelete;
    
    /**
     * Instantiates a new FieldDef.
     */
    public FieldDef()
    {
        _name = null;
        _fieldSize = -1;
        _nativeDataType = null;
        _sqlDataType = -1;
        _isNullable = true;
        _scale = -1;
        _precision = -1;
        _dataSize = null;
        _hasParams = false;
        _bestMatch = null;
        _encode = false;
        _visible = true;
        _index = -1;
        _isAutoIncrement = false;
        _versions = 1;
        _toDelete = false;
    }
    
    /**
     * Instantiates a new FieldDef.
     *
     * @param sqlDataType the sql data type, for example java.sql.Types.NUMERIC
     * @param nativeDataType the native data type, the native data type, for example "NUMBER(18)"
     */
    public FieldDef(int sqlDataType, String nativeDataType)
    {
        this();
        
        _sqlDataType = sqlDataType;
        
        _nativeDataType = nativeDataType;
    }
    
    /**
     * Add version
     */
    public void addVersion()
    {
        _versions++;
    }
    
    /**
     * Copy field.
     * 
     * @return copy of this field
     */
    public FieldDef copy()
    {
        FieldDef field = new FieldDef();
        
        field._name = _name;
        field._fieldSize = _fieldSize;
        field._nativeDataType = _nativeDataType;
        field._sqlDataType = _sqlDataType;
        field._isNullable = _isNullable;
        field._scale = _scale;
        field._precision = _precision;
        field._dataSize = _dataSize;
        field._hasParams = _hasParams;
        field._bestMatch = _bestMatch;
        field._encode = _encode;
        field._visible = _visible;
        field._index = _index;
        field._isAutoIncrement = _isAutoIncrement;
        field._versions = 1;
        field._toDelete = _toDelete;
        
        return field;
    }
    
    /**
     * Gets the best match. The best match is a field definition which matches the best with this one. 
     * Used by etl engine to substitute data types when moving data from one database to another (for example from Oracle to MySql).  
     *
     * @return the best match
     */
    public FieldDef getBestMatch()
    {
        return _bestMatch;
    }
    
    /**
     * Gets the data size.
     *
     * @return the data size
     */
    public String getDataSize()
    {
        return _dataSize;
    }
    
    /**
     * Gets the field size.
     *
     * @return the field size
     */
    public int getFieldSize()
    {
        return _fieldSize;
    }
    
    /**
     * Gets the index of the field.
     *
     * @return the index of the field
     */
    public int getIndex()
    {
        return _index;
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
     * Gets the native data type. For example "NUMBER(18)".
     *
     * @return the native data type
     */
    public String getNativeDataType()
    {
        return _nativeDataType;
    }
    
    /**
     * Gets the precision.
     *
     * @return the precision
     */
    public int getPrecision()
    {
        return _precision;
    }
    
    /**
     * Gets the scale.
     *
     * @return the scale
     */
    public int getScale()
    {
        return _scale;
    }
    
    /**
     * Gets the sql data type. For example java.sql.Types.NUMERIC
     *
     * @return the sql data type
     */
    public int getSqlDataType()
    {
        return _sqlDataType;
    }
    
    /**
     * Gets number of versions of the same field. The default value is 1.
     * 
     * @return number of versions
     */
    public int getVersions()
    {
        return _versions;
    }
    
    /**
     * Checks if field has parameters.
     *
     * @return true, if successful
     */
    public boolean hasParams()
    {
        return _hasParams;
    }
    
    /**
     * Checks if field is auto increment.
     *
     * @return true, if field is auto increment
     */
    public boolean isAutoIncrement()
    {
        return _isAutoIncrement;
    }
    
    /**
     * Checks if field is BLOB.
     *
     * @return true, if is BLOB
     */
    public boolean isBlob()
    {
        return SqlUtils.isBlob(_sqlDataType);
    }
    
    /**
     * Checks if field is boolean.
     *
     * @return true, if is boolean
     */
    public boolean isBoolean()
    {
        return SqlUtils.isBoolean(_sqlDataType);
    }
    
    /**
     * Checks if field is char (all flavors of chars).
     *
     * @return true, if is char
     */
    public boolean isChar()
    {
        return SqlUtils.isChar(_sqlDataType);
    }
    
    /**
     * Checks if field is CLOB.
     *
     * @return true, if is CLOB
     */
    public boolean isClob()
    {
        return SqlUtils.isClob(_sqlDataType);
    }
    
    /**
     * Checks if field is date or time or timestamp.
     *
     * @return true, if field is date or time or timestamp.
     */
    public boolean isDate()
    {
        return SqlUtils.isDate(_sqlDataType);
    }
    
    /**
     * Checks if field is date.
     *
     * @return true, if field is date
     */
    public boolean isDateOnly()
    {
        return SqlUtils.isDateOnly(_sqlDataType);
    }
    
    /**
     * Checks if field is date or timestamp.
     *
     * @return true, if field is date or timestamp.
     */
    public boolean isDateTime()
    {
        return SqlUtils.isDateTime(_sqlDataType);
    }
    
    /**
     * Checks if field is encoded.
     *
     * @return true, if is encoded
     */
    public boolean isEncode()
    {
        return _encode;
    }
    
    /**
     * Checks if field is CLOB or BLOB.
     *
     * @return true, if field is CLOB or BLOB
     */
    public boolean isLargeObject()
    {
        return SqlUtils.isLargeObject(_sqlDataType);
    }
    
    /**
     * Checks if field is mark to be deleted.
     * @return <code>true</code> if field is mark to be deleted
     */
    public boolean isMarkedToDelete()
    {
        return _toDelete;
    }
    
    /**
     * Checks if field is nullable.
     *
     * @return true, if is nullable
     */
    public boolean isNullable()
    {
        return _isNullable;
    }
    
    /**
     * Checks if field is numeric.
     *
     * @return true, if field is numeric
     */
    public boolean isNumeric()
    {
        return SqlUtils.isNumber(_sqlDataType);
    }
    
    /**
     * Checks if field is on the following data types: DISTINCT, REF, DATALINK.
     *
     * @return true, if true
     */
    public boolean isOther()
    {
        return SqlUtils.isOther(_sqlDataType);
    }
    
    /**
     * Checks if field is time.
     *
     * @return true, if field is time
     */
    public boolean isTime()
    {
        return SqlUtils.isTime(_sqlDataType);
    }
    
    /**
     * Checks if field is timestamp.
     *
     * @return true, if field is timestamp
     */
    public boolean isTimestamp()
    {
        return SqlUtils.isTimestamp(_sqlDataType);
    }
    
    /**
     * Checks if field is visible.
     *
     * @return true, if field is visible
     */
    public boolean isVisible()
    {
        return _visible;
    }
    
    /**
     * Remove version.
     */
    public void removeVersion()
    {
        if (_versions > 1)
            _versions--;
    }
    
    /**
     * Sets the auto increment flag for the field.
     *
     * @param value the new auto increment flag
     */
    public void setAutoIncrement(boolean value)
    {
        _isAutoIncrement = value;
    }
    
    /**
     * Sets the best match.  
     *
     * @param value the new best match
     */
    public void setBestMatch(FieldDef value)
    {
        _bestMatch = value;
    }
    
    /**
     * Sets the data size.
     *
     * @param value the new data size
     */
    public void setDataSize(String value)
    {
        _dataSize = value;
    }
    
    /**
     * Sets the encode flag.
     *
     * @param value the new encode flag
     */
    public void setEncode(boolean value)
    {
        _encode = value;
    }
    
    /**
     * Sets the field size.
     *
     * @param value the new field size
     */
    public void setFieldSize(int value)
    {
        _fieldSize = value;
    }
    
    /**
     * Sets the "has parameters" flag.
     *
     * @param value the new value for "has parameters" flag
     */
    public void setHasParams(boolean value)
    {
        _hasParams = value;
    }
    
    /**
     * Sets the index.
     *
     * @param value the new index
     */
    public void setIndex(int value)
    {
        _index = value;
    }
    
    /**
     * Sets the name.
     *
     * @param value the new name
     */
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the native data type.
     *
     * @param value the new native data type
     */
    public void setNativeDataType(String value)
    {
        _nativeDataType = value;
    }
    
    /**
     * Sets the nullable.
     *
     * @param value the new nullable
     */
    public void setNullable(boolean value)
    {
        _isNullable = value;
    }
    
    /**
     * Sets the precision.
     *
     * @param value the new precision
     */
    public void setPrecision(int value)
    {
        _precision = value;
    }
    
    /**
     * Sets the scale.
     *
     * @param value the new scale
     */
    public void setScale(int value)
    {
        _scale = value;
    }
    
    /**
     * Sets the sql data type.
     *
     * @param value the new sql data type
     */
    public void setSqlDataType(int value)
    {
        _sqlDataType = value;
    }
    
    /**
     * Sets "marked to be deleted" flag.
     *  
     * @param value the new value of the "marked to be deleted" flag 
     */
    public void setToDelete(boolean value)
    {
        _toDelete = value;
    }
    
    /**
     * Sets the "is visible" flag.
     *
     * @param value the new value for "is visible" flag
     */
    public void setVisible(boolean value)
    {
        _visible = value;
    }
    
}
