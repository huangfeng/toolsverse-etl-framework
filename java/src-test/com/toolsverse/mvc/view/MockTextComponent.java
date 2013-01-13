/*
 * MockTextComponent.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

import javax.swing.text.AbstractDocument;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.PlainDocument;

/**
 * MockTextComponent
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockTextComponent extends MockComponent
{
    private PlainDocument _document = new PlainDocument();
    
    public Document getDocument()
    {
        return _document;
    }
    
    public String getText()
    {
        Document doc = getDocument();
        String txt;
        try
        {
            txt = doc.getText(0, doc.getLength());
        }
        catch (BadLocationException e)
        {
            txt = null;
        }
        return txt;
    }
    
    public void setText(String t)
    {
        try
        {
            Document doc = getDocument();
            if (doc instanceof AbstractDocument)
            {
                ((AbstractDocument)doc).replace(0, doc.getLength(), t, null);
            }
            else
            {
                doc.remove(0, doc.getLength());
                doc.insertString(0, t, null);
            }
        }
        catch (BadLocationException e)
        {
        }
    }
}
