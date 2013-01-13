/*
 * Table.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import javax.swing.table.TableModel;

/**
 * Generic Table interface. If some class implements Table it is indication for the plug-in that it can attach itself to it. 
 * For example Connector plug-in can be attached to the class implementing Table.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface Table extends IGrid
{
    
    /**
     * Gets the selected column.
     *
     * @return the selected column
     */
    int getSelectedColumn();
    
    /**
     * Gets the selected row.
     *
     * @return the selected row
     */
    int getSelectedRow();
    
    /**
     * Gets the table model.
     *
     * @return the table model
     */
    TableModel getTableModel();
    
    /**
     * Sets the selected cell.
     *
     * @param row the row
     * @param col the column
     */
    void setSelectedCell(int row, int col);
    
    /**
     * Sets the selected row.
     *
     * @param index the new selected row
     */
    void setSelectedRow(int index);
}
