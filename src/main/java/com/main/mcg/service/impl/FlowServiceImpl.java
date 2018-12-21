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

package com.main.mcg.service.impl;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.main.mcg.common.Constants;
import com.main.mcg.common.sysenum.LogTypeEnum;
import com.main.mcg.common.sysenum.MessageTypeEnum;
import com.main.mcg.entity.common.SelectEntity;
import com.main.mcg.entity.flow.FlowStruct;
import com.main.mcg.entity.flow.data.DataRecord;
import com.main.mcg.entity.flow.gmybatis.Table;
import com.main.mcg.entity.flow.model.FlowModel;
import com.main.mcg.entity.flow.web.WebStruct;
import com.main.mcg.entity.generate.ExecuteStruct;
import com.main.mcg.entity.generate.RunResult;
import com.main.mcg.entity.generate.RunStatus;
import com.main.mcg.entity.global.McgGlobal;
import com.main.mcg.entity.global.datasource.McgDataSource;
import com.main.mcg.entity.message.Message;
import com.main.mcg.entity.message.NotifyBody;
import com.main.mcg.plugin.build.McgProduct;
import com.main.mcg.plugin.dbconn.FlowDataAdapterImpl;
import com.main.mcg.plugin.dbconn.McgBizAdapter;
import com.main.mcg.plugin.ehcache.CachePlugin;
import com.main.mcg.plugin.generate.FlowTask;
import com.main.mcg.plugin.websocket.MessagePlugin;
import com.main.mcg.service.FlowService;
import com.main.mcg.service.GlobalService;
import com.main.mcg.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.google.common.base.CaseFormat;


@Service
public class FlowServiceImpl implements FlowService
{

    @Autowired
    private GlobalService globalService;
    
    @Override
    public boolean testConnect(McgDataSource mcgDataSource) {
    	
    	McgBizAdapter mcgBizAdapter = new FlowDataAdapterImpl(mcgDataSource);
    	if("DB2".equals(mcgDataSource.getDbType())){
    		return mcgBizAdapter.testConnectDB2(mcgDataSource);
    	}else{
    		return mcgBizAdapter.testConnect();
    	}
        
    }

    @Override
	public List<McgDataSource> getMcgDataSources() throws ClassNotFoundException, IOException {
		List<McgDataSource> mcgDataSourceList = null;
		McgGlobal mcgGlobal = (McgGlobal) LevelDbUtil.getObject(Constants.GLOBAL_KEY, McgGlobal.class);
		if(mcgGlobal != null) {
			mcgDataSourceList = mcgGlobal.getFlowDataSources();
		}
		return mcgDataSourceList;
	}
	
	@Override
	public List<DataRecord> getTableInfo(McgDataSource mcgDataSource, String tableName) {
	    
	    McgBizAdapter mcgBizAdapter = new FlowDataAdapterImpl(mcgDataSource);
		List<DataRecord> result =  new ArrayList<DataRecord>();
		if("DB2".equals(mcgDataSource.getDbType())){	
			Connection conn = null;
	        try {
	            Class.forName("com.ibm.db2.jcc.DB2Driver");
	            String url = "jdbc:db2://" + mcgDataSource.getDbServer() + ":" +mcgDataSource.getDbPort() 
	                + "/"+mcgDataSource.getDbName();// url为连接字符串
	            
	            String user = mcgDataSource.getDbUserName();// 数据库用户名
	            String pwd = mcgDataSource.getDbPwd();// 数据库密码
	            conn = (Connection) DriverManager.getConnection(url, user, pwd);
	            System.out.println("数据库连接成功！！！");
	           
	            DatabaseMetaData dbmd = conn.getMetaData();
	    		ResultSet resultSet = dbmd.getTables(null, "%", tableName,
	    				new String[] { "TABLE" });

	    		ResultSet primaryKeyResultSet = dbmd.getPrimaryKeys(conn.getCatalog(),
	    				null, tableName);
	    		Map<String, String> primaryMap = new HashMap<String, String>();
	    		while (primaryKeyResultSet.next()) {
	    			String primaryKeyColumnName = primaryKeyResultSet
	    					.getString("COLUMN_NAME");
	    			primaryMap.put(primaryKeyColumnName, primaryKeyColumnName);
	    		}

	    		while (resultSet.next()) {
	    			String existTableName = resultSet.getString("TABLE_NAME");
	    			if (existTableName.equals(tableName)) {
	    				ResultSet rs = conn.getMetaData().getColumns(null, null,
	    						existTableName, "%");
	    				while (rs.next()) {
	    					DataRecord dataRecord = new DataRecord();
	    					dataRecord.setTableField(rs.getString("COLUMN_NAME")); // 字段名
	    					dataRecord.setClassField(Tools.convertFieldName(dataRecord
	    							.getTableField()));// 变量名
	    					dataRecord.setComment(rs.getString("REMARKS")); // 字段注释
	    					dataRecord.setTableFieldType(rs.getString("TYPE_NAME")); // 表字段数据类型
	    					dataRecord.setMandatory(!rs.getBoolean("NULLABLE"));// 非空
	    					dataRecord.setLength(rs.getInt("COLUMN_SIZE"));// 列大小
	    					dataRecord.setPrecision(rs.getInt("DECIMAL_DIGITS"));// 精度
	    					dataRecord.setAutoincrement("YES".equals(rs
	    							.getString("IS_AUTOINCREMENT")));// 是否自增
	    					dataRecord.setInclude(JDBCTypesUtils.jdbcTypeToJavaType(
	    							rs.getInt("DATA_TYPE")).getName()); // java import类型
	    																// Constants.javaTypeMap.get(rs.getInt("DATA_TYPE"))
	    					dataRecord.setDataType(Tools.splitLast(dataRecord
	    							.getInclude()));

	    					dataRecord
	    							.setPrimary(rs.getString("COLUMN_NAME")
	    									.equalsIgnoreCase(
	    											primaryMap.get(rs
	    													.getString("COLUMN_NAME")))); // 是否为主键
	    					// rs.getString("COLUMN_DEF"); //默认值
	    					// rs.getString("ORDINAL_POSITION")//序号
	    					result.add(dataRecord);
	    				}
	    			}
	    		}
	    		conn.close();
	        } catch (Exception e) {
	            System.out.println(e.getMessage());
	        }
		}else{
			try {
	        	result = mcgBizAdapter.getTableInfo(tableName);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		}
        
		return result;
	}

	@Override
	public List<Table> getTableByDataSource(McgDataSource mcgDataSource) {
		List<Table> result = null;
		if(mcgDataSource != null){
			if("DB2".equals(mcgDataSource.getDbType())){
				Connection con = null;
		        try {
		            Class.forName("com.ibm.db2.jcc.DB2Driver");
		            String url = "jdbc:db2://" + mcgDataSource.getDbServer() + ":" +mcgDataSource.getDbPort() 
		                + "/"+mcgDataSource.getDbName();// url为连接字符串
		            
		            String user = mcgDataSource.getDbUserName();// 数据库用户名
		            String pwd = mcgDataSource.getDbPwd();// 数据库密码
		            con = (Connection) DriverManager.getConnection(url, user, pwd);
		            System.out.println("数据库连接成功！！！");
		            /** 查询语句 **/
			        String sql = "SELECT  name tableName FROM SYSIBM.SYSTABLES";
			    	// 执行的SQL语句(含?号)
			        Statement stmt = (Statement) con.createStatement();
	                //PreparedStatement stmt2 = con.prepareStatement(sql2);
	                ResultSet rs=stmt.executeQuery(sql);
	                List<Table> tableList = new ArrayList<Table>(); 
	                while(rs.next()){
	                	Table t = new Table();
	                	t.setTableName(rs.getString("tableName"));
	                	tableList.add(t);
	               }
	               result = tableList;
	               con.close();
		        } catch (Exception e) {
		            System.out.println(e.getMessage());
		        }
			}else{
				McgBizAdapter mcgBizAdapter = new FlowDataAdapterImpl(mcgDataSource);
		        
		        try {
		            result = mcgBizAdapter.getTablesByDataSource(mcgDataSource.getDbName());
		        } catch (SQLException e) {
		            e.printStackTrace();
		        }
			}
		}

		if(result != null && result.size() > 0) {
		    for(Table table : result) {
		        String entityName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, table.getTableName());
		        table.setDaoName(entityName + "Dao");
		        table.setEntityName(entityName);
		        table.setXmlName(entityName + "Mapper");
		    }
		}
		return result;
	}
	
	@Override
	public McgDataSource getMcgDataSourceById(String dataSourceId) throws ClassNotFoundException, IOException {
		McgDataSource result = null;
		List<McgDataSource> mcgDataSourceList = getMcgDataSources();
		if(mcgDataSourceList == null) {
			return null;
		}
		for(McgDataSource mcgDataSource : mcgDataSourceList) {
			if(dataSourceId.equals(mcgDataSource.getDataSourceId())) {
				result = mcgDataSource;
				break;
			}
		}
		return result;
	}

	@Override
	public FlowStruct xmlToflowStruct(String flowXml) {
		FlowStruct flowStruct = null;
        try {  
            JAXBContext context = JAXBContext.newInstance(FlowStruct.class);  
            Unmarshaller unmarshaller = context.createUnmarshaller();  
            flowStruct = (FlowStruct)unmarshaller.unmarshal(new StringReader(flowXml));  
        } catch (JAXBException e) {  
            e.printStackTrace();  
        }		
		return flowStruct;
	}

    @Override
    public boolean saveFlow(WebStruct webStruct, HttpSession session) throws IOException {
        boolean result = false;
        FlowStruct flowStruct = DataConverter.webStructToflowStruct(webStruct);
        if(flowStruct == null) {
            globalService.saveFlowEmpty(webStruct.getFlowId());
        } else  {
            flowStruct.setMcgId(webStruct.getFlowId());
			LevelDbUtil.putObject(webStruct.getFlowId(), flowStruct);
        }
        result = true;
        return result;
    }

    @Override
    public boolean generate(WebStruct webStruct, HttpSession session) throws ClassNotFoundException, IOException {
        Message message = MessagePlugin.getMessage();
        message.getHeader().setMesType(MessageTypeEnum.NOTIFY);
        NotifyBody notifyBody = new NotifyBody();
        
        FlowStruct flowStruct = (FlowStruct)LevelDbUtil.getObject(webStruct.getFlowId(), FlowStruct.class);
        if(flowStruct != null) {

            Map<String, RunResult> runResultMap = new HashMap<String, RunResult>();
            TopoSort topoSort = new TopoSort();
            ExecuteStruct executeStruct = new ExecuteStruct();
            executeStruct.setDataMap(topoSort.init(topoSort, flowStruct));
            executeStruct.setOrders(topoSort.getFlowSort());
            executeStruct.setRunResultMap(runResultMap);
            RunStatus runStatus = new RunStatus();
            executeStruct.setRunStatus(runStatus);          
        
            notifyBody.setContent("流程执行开始！");
            notifyBody.setType(LogTypeEnum.INFO.getValue());
            message.setBody(notifyBody);
            MessagePlugin.push(session.getId(), message);
            FlowTask flowTask = new FlowTask(session.getId(), flowStruct, this, executeStruct);
            Thread thread = new Thread(flowTask, session.getId());
            thread.start();             
        } else {
            notifyBody.setContent("请先绘制流程！");
            notifyBody.setType(LogTypeEnum.ERROR.getValue());
            message.setBody(notifyBody);
            MessagePlugin.push(session.getId(), message);             
        }        
        return false;
    }

	@Override
	public boolean clearFileData(String path) {
		
		return McgFileUtils.clearFileData(path);
	}

    @Override
    public List<SelectEntity> getModelsByIds(List<String> ids) {
        List<SelectEntity> selectList = new ArrayList<SelectEntity>();
        for(String modelId : ids) {
            McgProduct mcgProduct = (McgProduct) CachePlugin.get(modelId);
            if(mcgProduct instanceof FlowModel) {
                FlowModel flowModel = (FlowModel)CachePlugin.get(modelId);
                SelectEntity select = new SelectEntity();
                select.setName(flowModel.getModelProperty().getModelName());
                select.setValue(flowModel.getModelId());
                selectList.add(select);
            }
        }
        return selectList;
    }
	
    
}