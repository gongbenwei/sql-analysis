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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
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
    public Object sqlResolve(HttpServletRequest request) {
        String sql= request.getParameter("sql");
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
            Query query = (Query)parser.createStatement(sql);
            QuerySpecification body = (QuerySpecification)query.getQueryBody();
            SqlAnalysis sqlVo = new SqlAnalysis();
            Select select = body.getSelect();
            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            Map<String,List<String>> tableColRelation = new HashMap<>();
            Relation relation = body.getFrom().get();
            //
            List<Node> nodes = getAllNode(relation);
            logger.info("nodes: "+nodes);
            columns = getColumns(select,columns);
            tables = traversalMany(nodes, tables);
            tableColRelation = getTableColRelation(tables,getAllColumn(sql));
            sqlVo.setColumns(columns);
            sqlVo.setTables(tables);
            sqlVo.setWheres(wheres);
            sqlVo.setTableColRelation(tableColRelation);
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
            columnsList.add(item.toString());
        }
        return columnsList;
    }

    //解析树，得到表名
    public List<String> traversal(Node node, List<String> tableList){
        if (node instanceof Table) {
            Table table = (Table)node;
            tableList.add(table.getName().toString());

        }
        if (node instanceof  AliasedRelation){
            AliasedRelation aliasedRelation = (AliasedRelation)node;
            Table table = (Table)aliasedRelation.getRelation();
            //Identifier identifier = aliasedRelation.getAlias();
            tableList.add(table.getName().toString());
        }
        return tableList;
    }

    //解析拿到所有的底层node
    public List<Node> getAllNode (Node node){
        List<Node> nodes = new ArrayList<>();
        if (node instanceof Join){
            Join join = (Join)node;
            Node leftNode = join.getLeft();
            Node rightNode = join.getRight();
            List<Node> arr;
            if (!(leftNode instanceof Join)){
                nodes.add(leftNode);
            }else{
                arr = getAllNode(leftNode);
                nodes.addAll(arr);
            }

            if (!(rightNode instanceof Join)){
                nodes.add(rightNode);
            }else{
                arr = getAllNode(rightNode);
                nodes.addAll(arr);
            }
        }else{
            nodes.add(node);
        }
        return nodes;
    }

    //解析树，得到表名，支持多node
    public List<String> traversalMany(List<Node> nodes, List<String> tableList){
        for (Node node : nodes){
            if (node instanceof Table) {
                Table table = (Table)node;
                tableList.add(table.getName().toString());

            }
            if (node instanceof  AliasedRelation){
                AliasedRelation aliasedRelation = (AliasedRelation)node;
                Table table = (Table)aliasedRelation.getRelation();
                Identifier identifier = aliasedRelation.getAlias();
                tableList.add(table.getName().toString()+" "+identifier.getValue());
            }
        }
        return tableList;
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
                String colName = column.split("\\.")[1];
                if (aliasedTable[0].equals(tabName)){
                    colList.add(colName);
                }
            }
            tabColRelationMAp.put(table.split(" ")[0],colList);
        }
        return tabColRelationMAp;
    }

    //利用alibaba-druid得到所有的列，函数也支持，要求SQL的列的写法是表名.列名
    public List<String> getAllColumn(String sql){
        List<String> allComluns = new ArrayList<>();
        SQLStatementParser parser = new HiveStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
        statement.accept(visitor);
        Collection<TableStat.Column> columns = visitor.getColumns();
        Iterator it=columns.iterator();
        while (it.hasNext()){
            allComluns.add(it.next().toString());
        }
        logger.info("alibaba-druid get col: "+allComluns);
        return allComluns;
    }
}
