/*
 * ListProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.util.List;

import com.toolsverse.util.KeyValue;

/**
 * This interface must be implemented by the classes which feed KeyValue combo boxes with data.
 *
 * @see com.toolsverse.ui.common.KeyValueComboBoxModel
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ListProvider
{
    
    /**
     * Gets the list of the KeyValue pairs. The key is name an the value is an actual object, for example etl driver. 
     *
     * @return the list
     */
    List<KeyValue> getList();
}
