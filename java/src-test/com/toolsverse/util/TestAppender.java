/*
 * TestAppender.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * TestAppender
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TestAppender extends AppenderSkeleton
{
    private Map<Object, String> _messages = new HashMap<Object, String>();
    
    @Override
    protected void append(LoggingEvent event)
    {
        _messages.put(event.getMessage(), event.getRenderedMessage());
    }
    
    public void clear()
    {
        _messages.clear();
    }
    
    public void close()
    {
    }
    
    public boolean containsMessage(Object message)
    {
        return _messages.containsKey(message);
    }
    
    public boolean requiresLayout()
    {
        return false;
    }
}
