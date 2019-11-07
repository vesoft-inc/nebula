/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.graph.client;

import com.vesoft.nebula.graph.RowValue;

import java.util.LinkedList;
import java.util.List;

public class ResultSet {

    private List<byte[]> columns;
    private List<RowValue> rows;

    public ResultSet() {
        this(new LinkedList<byte[]>(), new LinkedList<RowValue>());
    }

    public ResultSet(List<byte[]> columns, List<RowValue> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    public List<byte[]> getColumns() {
        return columns;
    }

    public List<RowValue> getRows() {
        return rows;
    }
}
