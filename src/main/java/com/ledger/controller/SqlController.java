package com.ledger.controller;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.ledger.common.Constant;
import com.ledger.common.NewResponse;
import com.ledger.common.SqlAnalysis;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/sql")
public class SqlController {

    private NewResponse response = new NewResponse();

    @ResponseBody
    @RequestMapping(value = "/analysis",method = RequestMethod.POST)
    public Object sqlResolve(HttpServletRequest request) {
        String sql= request.getParameter("sql");
        //将{{.}}替换成？，然后在进行解析
        sql = sql.replaceAll("\\{\\{(\\.{1}.+?)\\}\\}", "?");
        NewResponse response = new NewResponse();
        if (StringUtils.isEmpty(sql)){
            response.setCode("1");
            response.setMessage("tables : sql is null");
            return response;
        }
        try {
            SqlParser parser = new SqlParser();
            Query query = (Query)parser.createStatement(sql);
            QuerySpecification body = (QuerySpecification)query.getQueryBody();
            SqlAnalysis sqlVo = new SqlAnalysis();
            Select select = body.getSelect();
            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            Relation relation = body.getFrom().get();
            //
            columns = getColumns(select,columns);
            tables = traversal(relation, tables);
            sqlVo.setColumns(columns);
            sqlVo.setTables(tables);
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
}
