/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import pojo.User;

import java.util.ArrayList;
import java.util.List;

public class TableSqlJob {

	public static void main(String[] args) throws Exception {
		testStreamTable();
	}

	//批处理
	public static void testBatchTable() throws Exception{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
		DataSource<User> ds = env.fromCollection(getUserList());
		tableEnv.registerDataSet("user",ds);

		Table table = tableEnv.scan("user").select("*").where("id % 2 === 0");
		table.printSchema();
	}

	//流处理
	public static void testStreamTable() throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		DataStreamSource<User> ds = env.fromCollection(getUserList());
		tableEnv.registerDataStream("user",ds);

		Table table = tableEnv.scan("user").select("*").where("id % 2 === 0");
		table.printSchema();
	}

	public static List<User> getUserList(){
		List<User> list = new ArrayList();
		for(int i = 0;i<100;i++){
			list.add(new User(i,"name"+i));
		}
		return list;
	}
}
