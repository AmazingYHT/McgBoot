/*
 * @Copyright (c) 2018 缪聪(mcg-helper@qq.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");  
 * you may not use this file except in compliance with the License.  
 * You may obtain a copy of the License at  
 *     
 *     http://www.apache.org/licenses/LICENSE-2.0  
 *     
 * Unless required by applicable law or agreed to in writing, software  
 * distributed under the License is distributed on an "AS IS" BASIS,  
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and  
 * limitations under the License.
 */

package com.main.mcg.plugin.execute.strategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.main.mcg.common.sysenum.EletypeEnum;
import com.main.mcg.common.sysenum.LogTypeEnum;
import com.main.mcg.common.sysenum.MessageTypeEnum;
import com.main.mcg.entity.flow.sqlquery.FlowSqlQuery;
import com.main.mcg.entity.generate.ExecuteStruct;
import com.main.mcg.entity.generate.RunResult;
import com.main.mcg.entity.global.datasource.McgDataSource;
import com.main.mcg.entity.message.FlowBody;
import com.main.mcg.entity.message.Message;
import com.main.mcg.plugin.build.McgProduct;
import com.main.mcg.plugin.dbconn.FlowDataAdapterImpl;
import com.main.mcg.plugin.dbconn.McgBizAdapter;
import com.main.mcg.plugin.execute.ProcessStrategy;
import com.main.mcg.plugin.generate.FlowTask;
import com.main.mcg.plugin.websocket.MessagePlugin;
import com.main.mcg.service.FlowService;
import com.main.mcg.service.impl.FlowServiceImpl;
import com.main.mcg.util.DataConverter;

public class FlowSqlQueryStrategy implements ProcessStrategy
{

	@Override
	public void prepare(ArrayList<String> sequence, McgProduct mcgProduct,
			ExecuteStruct executeStruct) throws Exception {
		FlowSqlQuery flowSqlQuery = (FlowSqlQuery) mcgProduct;
		executeStruct.getRunStatus().setExecuteId(flowSqlQuery.getId());
	}

	@Override
	public RunResult run(McgProduct mcgProduct, ExecuteStruct executeStruct)
			throws Exception {

		FlowSqlQuery flowSqlQuery = (FlowSqlQuery) mcgProduct;
		JSON parentParam = DataConverter.getParentRunResult(
				flowSqlQuery.getId(), executeStruct);

		Message message = MessagePlugin.getMessage();
		message.getHeader().setMesType(MessageTypeEnum.FLOW);
		FlowBody flowBody = new FlowBody();
		flowBody.setEleType(EletypeEnum.SQLQUERY.getValue());
		flowBody.setEleTypeDesc(EletypeEnum.SQLQUERY.getName() + "--》"
				+ flowSqlQuery.getSqlQueryProperty().getName());
		flowBody.setEleId(flowSqlQuery.getId());
		flowBody.setComment("参数");

		if (parentParam == null) {
			flowBody.setContent("{}");
		} else {
			flowBody.setContent(JSON.toJSONString(parentParam, true));
		}
		flowBody.setLogType(LogTypeEnum.INFO.getValue());
		flowBody.setLogTypeDesc(LogTypeEnum.INFO.getName());
		message.setBody(flowBody);
		FlowTask flowTask = FlowTask.executeLocal.get();
		MessagePlugin.push(flowTask.getHttpSessionId(), message);

		flowSqlQuery = DataConverter
				.flowOjbectRepalceGlobal(DataConverter.addFlowStartRunResult(
						parentParam, executeStruct), flowSqlQuery);
		RunResult runResult = new RunResult();
		runResult.setElementId(flowSqlQuery.getId());

		FlowService flowService = new FlowServiceImpl();

		List<Map<String, Object>> result = null;
		if ("DB2".equals(flowSqlQuery.getSqlQueryProperty().getDesc())) {
			McgDataSource mcgDataSource = flowService
					.getMcgDataSourceById(flowSqlQuery.getSqlQueryCore()
							.getDataSourceId());
			Connection con = null;
			try {
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				String url = "jdbc:db2://" + mcgDataSource.getDbServer() + ":"
						+ mcgDataSource.getDbPort() + "/"
						+ mcgDataSource.getDbName();// url为连接字符串
				String user = mcgDataSource.getDbUserName();// 数据库用户名
				String pwd = mcgDataSource.getDbPwd();// 数据库密码
				con = (Connection) DriverManager.getConnection(url, user, pwd);
				System.out.println("数据库连接成功！！！");

				/** 查询语句 **/
				String sql = flowSqlQuery.getSqlQueryCore().getSource();
				// 执行的SQL语句
				Statement stmt = (Statement) con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				result = resultSetToList(rs);	
				con.close();

			} catch (Exception e) {
				System.out.println("数据库连接失败" + e.getMessage());
			}
		} else {
			McgBizAdapter mcgBizAdapter = new FlowDataAdapterImpl(
					flowService.getMcgDataSourceById(flowSqlQuery
							.getSqlQueryCore().getDataSourceId()));
			result = mcgBizAdapter.tableQuery(flowSqlQuery.getSqlQueryCore().getSource(), null);
		}
		Map<String, List<Map<String, Object>>> map = new HashMap<String, List<Map<String, Object>>>();
		map.put(flowSqlQuery.getSqlQueryProperty().getKey(), result);
		runResult.setJsonVar(JSON.toJSONString(map,
				SerializerFeature.WriteDateUseDateFormat));
		executeStruct.getRunStatus().setCode("success");
		return runResult;
	}

	private List<Map<String, Object>> resultSetToList(ResultSet rs)throws SQLException {
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int colCount = rsmd.getColumnCount();
		List<String> colNameList = new ArrayList<String>();
		for (int i = 0; i < colCount; i++) {
			colNameList.add(rsmd.getColumnName(i + 1));
		}
		while (rs.next()) {
			Map map = new HashMap<String, Object>();
			for (int i = 0; i < colCount; i++) {
				String key = colNameList.get(i);
				Object value = rs.getString(colNameList.get(i));
				map.put(key, value);
			}
			results.add(map);
		}
		return results;
	}
}