package com.ledger.controller;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.ledger.common.Constant;
import com.ledger.common.NewResponse;
import com.ledger.common.SqlAnalysis;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/sql")
public class SqlController {

    protected static final Logger logger = LoggerFactory.getLogger(SqlController.class.getName());

    private NewResponse response = new NewResponse();

    @ResponseBody
    @RequestMapping(value = "/analysis",method = RequestMethod.POST)
    public Object sqlResolve(@RequestBody SqlAnalysis sqlAnalysis) {
        String sql= sqlAnalysis.getSql();
        logger.info("sql: "+sql);
        NewResponse response = new NewResponse();
        if (StringUtils.isEmpty(sql)){
            response.setCode("1");
            response.setMessage("tables : sql is null");
            return response;
        }
        try {
            //首先需要获取where条件
            String patternString = "\\{\\{(\\.{1}.+?)\\}\\}";
            Pattern pattern = Pattern.compile(patternString);
            Matcher matcher = pattern.matcher(sql);
            List<String> wheres = new ArrayList<>();
            while(matcher.find()) {
                String matchStr = matcher.group();
                wheres.add(matchStr.substring(3,matchStr.length()-2));
            }
            //将{{.}}替换成？，然后在进行解析
            sql = sql.replaceAll(patternString, "?");
            SqlParser parser = new SqlParser();
            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            Query query = (Query)parser.createStatement(sql);
            logger.info("queryBody: "+query.getQueryBody().getChildren());
            SqlAnalysis sqlVo = new SqlAnalysis();
            Map<String,List<String>> tableColRelation = new HashMap<>();
            HiveSchemaStatVisitor visitor = getVisitor(sql);
            tables = getAllTable(visitor);
            if (query.getQueryBody().getChildren().get(0) instanceof Select){
                QuerySpecification body = (QuerySpecification)query.getQueryBody();
                Select select = body.getSelect();
                columns = getColumns(select,columns);
            }
            if(query.getQueryBody().getChildren().get(0) instanceof QuerySpecification){
                for (int i=0; i<query.getQueryBody().getChildren().size(); i++){
                    QuerySpecification body = (QuerySpecification)query.getQueryBody().getChildren().get(i);
                    Select select = body.getSelect();
                    columns = getColumns(select,columns);
                }
            }
            tableColRelation = getTableColRelation(tables,getAllColumn(visitor));
            sqlVo.setColumns(columns);
            sqlVo.setTables(tables);
            sqlVo.setWheres(wheres);
            sqlVo.setTableColRelation(tableColRelation);
            sqlVo.setSql(sql);
            logger.info("sqlVo: "+sqlVo);
            response.setData(sqlVo);
        }catch (Exception e){
            response.setCode(Constant.Failure);
            response.setMessage("err:"+e.getMessage());
        }
        return response;
    }

    //解析拿到列
    public List<String> getColumns(Select select, List<String> columnsList){
        for (SelectItem item: select.getSelectItems()){
            if (columnsList.contains(item.toString())){
                continue;
            }
            columnsList.add(item.toString());
        }
        return columnsList;
    }

    //根据sql语句，拿到table与字段的对应关系，要求sql写法满足table.colName
    public Map<String,List<String>> getTableColRelation(List<String> tables, List<String> columns){
        Map tabColRelationMAp = new HashMap<String,List<String>>();
        for (String table : tables){
            List<String> colList = new ArrayList<>();
            String [] aliasedTable = table.split(" ");
            for (String column : columns){
                //取得不带别名的列-表名.列名
                //String originalCol = column.split(" ")[0];
                //将表名.列名根据中间.进行切割
                String tabName = column.split("\\.")[0];
                if (aliasedTable[0].equals(tabName)){
                    String colName = column.split("\\.")[1];
                    colList.add(colName);
                }
            }
            tabColRelationMAp.put(table.split(" ")[0],colList);
        }
        return tabColRelationMAp;
    }

    //利用alibaba-druid获取得到druid的visitor对象
    public HiveSchemaStatVisitor getVisitor(String sql){
        SQLStatementParser parser = new HiveStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
        statement.accept(visitor);
        return visitor;
    }

    //从visitor对象中得到所有的表
    static List<String> getAllTable(HiveSchemaStatVisitor visitor){
        List<String> allTables = new ArrayList<>();
        HashMap<TableStat.Name, TableStat> tables = (HashMap<TableStat.Name, TableStat>) visitor.getTables();
        for (TableStat.Name tabName : tables.keySet()){
            allTables.add(tabName.toString());
        }
        logger.info("alibaba-druid get allTabs: "+allTables);
        return allTables;
    }

    //从visitor对象中得到所有的列，函数也支持，要求SQL的列的写法是表名.列名
    public List<String> getAllColumn(HiveSchemaStatVisitor visitor){
        List<String> allColumns = new ArrayList<>();
        Collection<TableStat.Column> columns = visitor.getColumns();
        Iterator it=columns.iterator();
        while (it.hasNext()){
            allColumns.add(it.next().toString());
        }
        logger.info("alibaba-druid get col: "+allColumns);
        return allColumns;
    }
}
