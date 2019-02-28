/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client;

import com.google.common.collect.Lists;

import java.util.List;

import nebula.graph.RowValue;

public class ResultSet {

    private List<byte[]> columns;
    private List<RowValue> rows;

    public ResultSet() {
        this(Lists.newArrayList(), Lists.newArrayList());
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
