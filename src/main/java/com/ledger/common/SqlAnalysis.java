package com.ledger.common;

import javax.annotation.Resource;
import java.util.List;

@Resource
public class SqlAnalysis {

    private List<String> columns;

    private List<String> tables;

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    //重写toString();
    public String toString(){
        return "columns:"+columns+";tables:"+tables;
    }
}
