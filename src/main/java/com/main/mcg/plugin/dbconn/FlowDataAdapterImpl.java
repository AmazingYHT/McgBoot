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

package com.main.mcg.plugin.dbconn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.main.mcg.common.sysenum.DatabaseTypeEnum;
import com.main.mcg.entity.global.datasource.McgDataSource;
import com.main.mcg.plugin.dbconn.mysql.MysqlConnectImpl;
import com.main.mcg.plugin.dbconn.oracle.OracleConnectImpl;
import com.main.mcg.plugin.dbconn.postgresql.PostgresqlConnectImpl;
import org.postgresql.ds.PGPoolingDataSource;






public class FlowDataAdapterImpl implements McgBizAdapter {

    private McgConnect mcgConnect;
    
    public FlowDataAdapterImpl(McgDataSource mcgDataSource) {
    	if(mcgDataSource != null){
	        if(DatabaseTypeEnum.MYSQL.getValue().equals(mcgDataSource.getDbType())) {
	            MysqlDataSource mysqlDataSource = new MysqlDataSource();
	            mysqlDataSource.setServerName(mcgDataSource.getDbServer());
	            mysqlDataSource.setPort(mcgDataSource.getDbPort());
	            mysqlDataSource.setDatabaseName(mcgDataSource.getDbName());
	            mysqlDataSource.setCharacterEncoding("UTF-8");
	            mysqlDataSource.setCharacterSetResults("UTF-8");
	            mysqlDataSource.setUser(mcgDataSource.getDbUserName());
	            mysqlDataSource.setPassword(mcgDataSource.getDbPwd());
	            try {
					mysqlDataSource.setConnectTimeout(3000);
				} catch (SQLException e) {
					e.printStackTrace();
				}
	            mcgConnect = new MysqlConnectImpl(mysqlDataSource);
	        } else if(DatabaseTypeEnum.ORACLE.getValue().equals(mcgDataSource.getDbType())) {
	            try {
	                OracleDataSource oracleDataSource = new OracleDataSource();
	                oracleDataSource.setDriverType("thin"); 
	                oracleDataSource.setNetworkProtocol("tcp"); 
	                oracleDataSource.setServerName(mcgDataSource.getDbServer()); 
	                oracleDataSource.setDatabaseName(mcgDataSource.getDbName()); 
	                oracleDataSource.setPortNumber(mcgDataSource.getDbPort()); 
	                oracleDataSource.setUser(mcgDataSource.getDbUserName()); 
	                oracleDataSource.setPassword(mcgDataSource.getDbPwd()); 
	                oracleDataSource.setLoginTimeout(3000);
	                mcgConnect = new OracleConnectImpl(oracleDataSource);
	            } catch (SQLException e) {
	                e.printStackTrace();
	            }
	        } else if(DatabaseTypeEnum.MSSQL.getValue().equals(mcgDataSource.getDbType())) {
	            SQLServerDataSource sqlServerDataSource = new SQLServerDataSource();
	            sqlServerDataSource.setDatabaseName(mcgDataSource.getDbName());
	            sqlServerDataSource.setUser(mcgDataSource.getDbUserName());
	            sqlServerDataSource.setPassword(mcgDataSource.getDbPwd());
	            sqlServerDataSource.setServerName(mcgDataSource.getDbServer());
	            sqlServerDataSource.setPortNumber(mcgDataSource.getDbPort());
	            sqlServerDataSource.setLoginTimeout(3000);
	            mcgConnect = new MssqlConnectImpl(sqlServerDataSource);
	        } else if(DatabaseTypeEnum.POSTGRESQL.getValue().equals(mcgDataSource.getDbType())) {
	            PGPoolingDataSource pgDataSource = new PGPoolingDataSource();
	            pgDataSource.setDatabaseName(mcgDataSource.getDbName());
	            pgDataSource.setUser(mcgDataSource.getDbUserName());
	            pgDataSource.setPassword(mcgDataSource.getDbPwd());
	            pgDataSource.setPortNumber(mcgDataSource.getDbPort());
	            pgDataSource.setServerName(mcgDataSource.getDbServer());
	            try {
					pgDataSource.setLoginTimeout(3000);
				} catch (SQLException e) {
					e.printStackTrace();
				}
	            mcgConnect = new PostgresqlConnectImpl(pgDataSource);
	        } else if(DatabaseTypeEnum.DB2.getValue().equals(mcgDataSource.getDbType())) {
	        	mcgConnect = null;	
	        }   
    	}
    }
    
    @Override
    public List<DataRecord> getTableInfo(String tableName) throws Exception {
        
        return mcgConnect.getTableInfo(tableName);
    }

    @Override
    public List<Table> getTablesByDataSource(String dbName) throws SQLException {
        
        return mcgConnect.getTablesByDataSource(dbName);
    }

    @Override
    public List<Map<String, Object>> tableQuery(String sql, Object... para) throws SQLException {
        
        return mcgConnect.querySql(sql, para);
    }

    @Override
    public int executeUpdate(String sql, Object... para) throws SQLException {
        
        return mcgConnect.executeUpdate(sql, para);
    }

    @Override
    public boolean testConnect() {
        return mcgConnect.testConnect();
    }

	@Override
	public boolean testConnectDB2(McgDataSource mcgDataSource) {
        Connection con = null;
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            String url = "jdbc:db2://" + mcgDataSource.getDbServer() + ":" +mcgDataSource.getDbPort() 
                + "/"+mcgDataSource.getDbName();// url为连接字符串
            
            String user = mcgDataSource.getDbUserName();// 数据库用户名
            String pwd = mcgDataSource.getDbPwd();// 数据库密码
            con = (Connection) DriverManager.getConnection(url, user, pwd);
            System.out.println("数据库连接成功！！！");
            con.close();
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
		return false;
	}
    
    
}