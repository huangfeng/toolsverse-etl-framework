/*
 * KeyStrokeModel.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.KeyStroke;

import com.toolsverse.util.KeyValue;
import com.toolsverse.util.Utils;

/**
 * This class is used by UI component for configuring keyboard shortcuts.
 * Returns combo box models for keys (A,B,C, etc) and modifiers (Ctrl, Alt,
 * etc).
 * 
 * @see com.toolsverse.ui.common.KeyValueComboBoxModel
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class KeyStrokeModel
{
    
    /** The list of modifiers, such as Ctrl, Alt, etc. */
    private static List<KeyValue> listOfModifiers = new ArrayList<KeyValue>();
    static
    {
        listOfModifiers.add(new KeyValue(0, "None"));
        
        listOfModifiers.add(new KeyValue(InputEvent.CTRL_DOWN_MASK, "Ctrl"));
        listOfModifiers.add(new KeyValue(InputEvent.ALT_DOWN_MASK, "Alt"));
        listOfModifiers.add(new KeyValue(InputEvent.SHIFT_DOWN_MASK, "Shift"));
        listOfModifiers.add(new KeyValue(InputEvent.META_DOWN_MASK, "Meta"));
    }
    
    /** The modifiers. */
    private final KeyValueComboBoxModel _modifiers;
    
    /** The list of keys, such as A,B,C,etc */
    private static List<KeyValue> listOfKeys = new ArrayList<KeyValue>();
    static
    {
        listOfKeys.add(new KeyValue(-1, "None"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_0, "0"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_1, "1"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_2, "2"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_3, "3"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_4, "4"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_5, "5"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_6, "6"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_7, "7"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_8, "8"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_9, "9"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_ENTER, "Enter"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_BACK_SPACE, "Back Space"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_TAB, "Tab"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_CANCEL, "Cancel"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_CLEAR, "Clear"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_PAUSE, "Pause"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_CAPS_LOCK, "Caps Lock"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_ESCAPE, "Escape"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_SPACE, "Space"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_PAGE_UP, "Page Up"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_PAGE_DOWN, "Page Down"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_END, "End"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_HOME, "Home"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_LEFT, "Left"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_UP, "Up"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_RIGHT, "Right"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_DOWN, "Down"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_COMMA, "Comma"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_MINUS, "Minus"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_PERIOD, "Period"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_SLASH, "Slash"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_INSERT, "Insert"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_OPEN_BRACKET, "["));
        listOfKeys.add(new KeyValue(KeyEvent.VK_CLOSE_BRACKET, "]"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_A, "A"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_B, "B"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_C, "C"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_D, "D"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_E, "E"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F, "F"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_G, "G"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_H, "H"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_I, "I"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_J, "J"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_K, "K"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_L, "L"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_M, "M"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_N, "N"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_O, "O"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_P, "P"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_Q, "Q"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_R, "R"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_S, "S"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_T, "T"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_U, "U"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_V, "V"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_W, "W"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_X, "X"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_Y, "Y"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_Z, "Z"));
        
        listOfKeys.add(new KeyValue(KeyEvent.VK_F1, "F1"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F2, "F2"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F3, "F3"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F4, "F4"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F5, "F5"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F6, "F6"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F7, "F7"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F8, "F8"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F9, "F9"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F10, "F10"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F11, "F11"));
        listOfKeys.add(new KeyValue(KeyEvent.VK_F12, "F12"));
    }
    
    /** The combo box model. */
    private final KeyValueComboBoxModel _keys;
    
    /** The key stroke. */
    private KeyStroke _keyStroke;
    
    /**
     * Instantiates a new KeyStrokeModel.
     */
    public KeyStrokeModel()
    {
        _keyStroke = null;
        
        _keys = new KeyValueComboBoxModel(listOfKeys);
        _modifiers = new KeyValueComboBoxModel(listOfModifiers);
    }
    
    /**
     * Gets the key code.
     * 
     * @return the key code
     */
    public int getKeyCode()
    {
        return _keyStroke != null ? _keyStroke.getKeyCode() : -1;
    }
    
    /**
     * Gets the combo box model for keys (A,B,C, etc).
     * 
     * @return the combo box model for keys).
     */
    public KeyValueComboBoxModel getKeys()
    {
        return _keys;
    }
    
    /**
     * Gets the key stroke.
     * 
     * @return the key stroke
     */
    public KeyStroke getKeyStroke()
    {
        return _keyStroke;
    }
    
    /**
     * Gets the modifier.
     * 
     * @return the modifier
     */
    public int getModifier()
    {
        if (_keyStroke == null)
            return 0;
        
        int modifiers = _keyStroke.getModifiers();
        
        if (modifiers == 0)
            return 0;
        
        if ((modifiers & InputEvent.SHIFT_DOWN_MASK) != 0)
        {
            return InputEvent.SHIFT_DOWN_MASK;
        }
        else if ((modifiers & InputEvent.CTRL_DOWN_MASK) != 0)
        {
            return InputEvent.CTRL_DOWN_MASK;
        }
        else if ((modifiers & InputEvent.META_DOWN_MASK) != 0)
        {
            return InputEvent.META_DOWN_MASK;
        }
        else if ((modifiers & InputEvent.ALT_DOWN_MASK) != 0)
        {
            return InputEvent.ALT_DOWN_MASK;
        }
        
        return 0;
    }
    
    /**
     * Gets the combo box model for modifiers (Ctrl, Alt, etc).
     * 
     * @return the the combo box model for modifiers
     */
    public KeyValueComboBoxModel getModifiers()
    {
        return _modifiers;
    }
    
    /**
     * Sets the key code.
     * 
     * @param keyCode
     *            the new key code
     */
    public void setKeyCode(int keyCode)
    {
        if (_keyStroke == null)
        {
            _keyStroke = KeyStroke.getKeyStroke(keyCode, 0, false);
            
            return;
        }
        
        _keyStroke = KeyStroke.getKeyStroke(keyCode, getModifier(), false);
    }
    
    /**
     * Sets the key code.
     * 
     * @param key
     *            the new key code
     */
    public void setKeyCode(String key)
    {
        Integer keyCode = (Integer)_keys.getKeyByValue(key);
        
        if (keyCode == null)
        {
            _keyStroke = null;
            
            return;
        }
        
        setKeyCode(keyCode.intValue());
    }
    
    /**
     * Sets the key stroke.
     * 
     * @param modifier
     *            the modifier
     * @param keyCode
     *            the key code
     */
    public void setKeyStroke(int modifier, int keyCode)
    {
        _keyStroke = KeyStroke.getKeyStroke(keyCode, modifier, false);
    }
    
    /**
     * Sets the key stroke.
     * 
     * @param keyStroke
     *            the new key stroke
     */
    public void setKeyStroke(KeyStroke keyStroke)
    {
        _keyStroke = keyStroke;
    }
    
    /**
     * Sets the key stroke.
     * 
     * @param keyStroke
     *            the new key stroke
     */
    public void setKeyStroke(String keyStroke)
    {
        _keyStroke = str2KeyStroke(keyStroke);
    }
    
    /**
     * Sets the modifier.
     * 
     * @param modifier
     *            the new modifier
     */
    public void setModifier(int modifier)
    {
        if (_keyStroke == null)
            return;
        
        _keyStroke = KeyStroke.getKeyStroke(_keyStroke.getKeyCode(), modifier,
                false);
    }
    
    /**
     * Sets the modifier.
     * 
     * @param modifier
     *            the new modifier
     */
    public void setModifier(String modifier)
    {
        Integer modif = (Integer)_modifiers.getKeyByValue(modifier);
        
        setModifier(modif != null ? modif.intValue() : 0);
    }
    
    /**
     * Str2 key stroke.
     * 
     * @param value
     *            the value
     * @return the key stroke
     */
    public KeyStroke str2KeyStroke(String value)
    {
        if (Utils.isNothing(value))
        {
            return null;
        }
        
        String[] tokens = value.split(" ", -1);
        
        if (tokens.length == 1)
        {
            Integer key = (Integer)_keys.getKeyByValue(tokens[0]);
            
            if (key == null)
            {
                return null;
            }
            
            return KeyStroke.getKeyStroke(key.intValue(), 0, false);
        }
        
        Integer key = (Integer)_keys.getKeyByValue(tokens[1]);
        
        if (key == null)
        {
            return null;
        }
        
        Integer modifier = (Integer)_modifiers.getKeyByValue(tokens[0]);
        
        return KeyStroke.getKeyStroke(key.intValue(),
                modifier != null ? modifier.intValue() : 0, false);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        if (_keyStroke == null)
            return null;
        
        String key = (String)_keys.getValueByKey(getKeyCode());
        
        if (Utils.isNothing(key))
            return null;
        
        String modifier = (String)_modifiers.getValueByKey(getModifier());
        
        if (!Utils.isNothing(modifier))
            modifier = modifier + " ";
        else
            modifier = "";
        
        return modifier + key;
    }
    
}
