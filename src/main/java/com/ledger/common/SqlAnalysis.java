package com.ledger.common;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Resource
public class SqlAnalysis {

    private String sql;

    private List<String> columns;

    private List<String> tables;

    private List<String> wheres;

    private Map<String,List<String>> tableColRelation;

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

    public Map<String, List<String>> getTableColRelation() {
        return tableColRelation;
    }

    public void setTableColRelation(Map<String, List<String>> tableColRelation) {
        this.tableColRelation = tableColRelation;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    //重写toString();
    public String toString(){
        return "columns:"+columns+";\ntables:"+tables+";\nwheres:"+wheres+";\ntableColRelation:"+tableColRelation+";\nsql:"+sql;
    }
}
