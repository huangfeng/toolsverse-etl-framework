/*
 * IndexList.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.List;

/**
 * A List which extends Mutable interface and provides the lists of the deleted and inserted elements.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public interface UpdateList<E> extends List<E>, Mutable
{
    
    /**
     * Gets the list of the deleted elements
     * 
     * @return the list of the deleted elements
     */
    List<E> getDeletes();
    
    /**
     * Gets the list of the inserted elements
     * 
     * @return the list of the inserted elements
     */
    List<E> getInserts();
}