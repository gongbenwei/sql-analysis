package com.ledger.common;

import javax.annotation.Resource;
import java.util.List;

@Resource
public class SqlAnalysis {

    private List<String> columns;

    private List<String> tables;

    private List<String> wheres;

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

    public List<String> getWheres() {
        return wheres;
    }

    public void setWheres(List<String> wheres) {
        this.wheres = wheres;
    }

    //重写toString();
    public String toString(){
        return "columns:"+columns+";tables:"+tables+";wheres:"+wheres;
    }
}
