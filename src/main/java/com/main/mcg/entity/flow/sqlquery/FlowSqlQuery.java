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

package com.main.mcg.entity.flow.sqlquery;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import javax.validation.Valid;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.main.mcg.entity.flow.FlowBase;
import com.main.mcg.entity.generate.ExecuteStruct;
import com.main.mcg.entity.generate.RunResult;
import com.main.mcg.plugin.assist.ExceptionProcess;
import com.main.mcg.plugin.execute.ProcessContext;
import com.main.mcg.plugin.execute.strategy.FlowSqlQueryStrategy;
import org.hibernate.validator.constraints.NotBlank;


@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement
public class FlowSqlQuery extends FlowBase
{

    private static final long serialVersionUID = -7721981742663194475L;
    @NotBlank(message = "{sqlQuery.id.notBlank}")
    @XmlAttribute
    private String id;
    
    @Valid
    @XmlElement
    private SqlQueryProperty sqlQueryProperty;
    
    @Valid
    @XmlElement
    private SqlQueryCore sqlQueryCore;
    
    @Override
    public void prepare(ArrayList<String> sequence, ExecuteStruct executeStruct) {
        ProcessContext processContext = new ProcessContext();
        processContext.setProcessStrategy(new FlowSqlQueryStrategy());
        try {
            processContext.prepare(sequence, this, executeStruct);
        } catch (Exception e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(baos));  
            String exception = baos.toString();  
            ExceptionProcess.execute(this, exception);
        }   
        
    }

    @Override
    public RunResult execute(ExecuteStruct executeStruct) {
        ProcessContext processContext = new ProcessContext();
        processContext.setProcessStrategy(new FlowSqlQueryStrategy());
        RunResult runResult = null;
        try {
            runResult = processContext.run(this, executeStruct);
        } catch (Exception e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(baos));  
            String exception = baos.toString();  
            ExceptionProcess.execute(this, exception);
        }
        return runResult;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public SqlQueryProperty getSqlQueryProperty() {
        return sqlQueryProperty;
    }

    public void setSqlQueryProperty(SqlQueryProperty sqlQueryProperty) {
        this.sqlQueryProperty = sqlQueryProperty;
    }

    public SqlQueryCore getSqlQueryCore() {
        return sqlQueryCore;
    }

    public void setSqlQueryCore(SqlQueryCore sqlQueryCore) {
        this.sqlQueryCore = sqlQueryCore;
    }
    
}