package com.ledger.controller;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.ledger.common.Constant;
import com.ledger.common.NewResponse;
import com.ledger.common.SqlAnalysis;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller
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
            Relation relation = body.getFrom().get();
            //
            List<Node> nodes = getAllNode(relation);
            logger.info("nodes: "+nodes);
            columns = getColumns(select,columns);
            tables = traversalMany(nodes, tables);
            sqlVo.setColumns(columns);
            sqlVo.setTables(tables);
            sqlVo.setWheres(wheres);
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
}
