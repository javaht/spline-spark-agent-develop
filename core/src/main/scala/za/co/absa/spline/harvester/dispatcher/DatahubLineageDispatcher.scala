/*
 * Copyright 2021 ABSA Group Limited
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
 * 
 */

package za.co.absa.spline.harvester.dispatcher
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.protocol.HTTP

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.nio.charset.Charset
import scala.collection.mutable

class DatahubLineageDispatcher() extends AbstractJsonLineageDispatcher {

  override def name = "Datahub"

  override protected def send(data: String): Unit = {
    if (data.startsWith("ExecutionPlan")) {
      val replaceDate = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
      //这里拼接的血缘
      val sb = makeLine(replaceDate)
      //先删除原来的数据血缘
      val splineRemove = "{\"query\": \"mutation updateLineage { updateLineage( input:{ edgesToAdd : [],edgesToRemove: [" + sb + "]})}\",\"variables\":{}}"
      //增加最新的数据血缘
      val splineAdd = "{\"query\": \"mutation updateLineage { updateLineage( input:{ edgesToAdd : [" + sb + "],edgesToRemove: []})}\",\"variables\":{}}"
      handleHttp(splineRemove, "http://172.18.1.54:9002/api/graphql")
      handleHttp(splineAdd, "http://172.18.1.54:9002/api/graphql")
    }

  }

  def makeLine(replaceDate: String): StringBuilder = {
    var downstreamUrn: String = ""

    val operations: JSONObject  = JSON.parseObject(replaceDate).getJSONObject("operations");

    val write: JSONObject = operations.getJSONObject("write")
    val `type`: String = write.getJSONObject("extra").getString("destinationType")
    if ("hudi" == `type`) {
      val params: JSONObject = write.getJSONObject("params")
      //目标数据库
      val targetDatabase: String = params.getString("hoodie.datasource.hive_sync.database")
      //目标表
      val targetTablename: String = params.getString("hoodie.datasource.hive_sync.table")
      downstreamUrn = "{downstreamUrn: \\\"urn:li:dataset:(urn:li:dataPlatform:hive," + targetDatabase + "." + targetTablename + ",PROD)\\\","
    }
    val readsArray: JSONArray = operations.getJSONArray("reads") //这里开始获取数据来源

    val sourset: mutable.LinkedHashSet[String] = new mutable.LinkedHashSet[String] //定义一个不允许重复的集合

    for (i <- 0 until readsArray.size) {
      val readObj: JSONObject = readsArray.getJSONObject(i)
      val sourceTable: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("table")
      val sourceDatabase: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("database")
      sourset.add(sourceDatabase + "." + sourceTable)
    }

    val jsonParamList = mutable.LinkedHashSet[String]();

    for (s <- sourset) {
      val upstreamUrn: String = "upstreamUrn :  \\\"urn:li:dataset:(urn:li:dataPlatform:hive," + s + ",PROD)\\\"}"
      val jsonParam: String = downstreamUrn + upstreamUrn
      jsonParamList.add(jsonParam)
    }
    val sb: StringBuilder = new StringBuilder

    jsonParamList.foreach(
      row=>{
        sb.append(row).append(",")
      }
    )
    if(!jsonParamList.isEmpty) {
      sb.append(jsonParamList.last)
    }
    sb
  }


  def handleHttp(jsonParam: String, url: String): Unit = {
    var in: BufferedReader = null
    try {
      val client = HttpClients.createDefault
      val request = new HttpPost(url)
      //这里的token每组不一样 需要数据组长生成添加使用。
      val token = "x.x.x"
      request.addHeader(HTTP.CONTENT_TYPE, "application/json")
      request.addHeader("Authorization", "Bearer " + token)
      val s = new StringEntity(jsonParam, Charset.forName("UTF-8"))
      s.setContentEncoding("UTF-8")
      s.setContentType("application/json;charset=UTF-8")
      request.setEntity(s)
      val response = client.execute(request)
      val code = response.getStatusLine.getStatusCode

      val in = response.getEntity.getContent.read()

      if (code == 200) {
        System.out.println("接口请求成功:" + in.toString + "        " + url)
      } else if (code == 500){
        System.out.println("服务器错误:" + in + ",url:" + url)
      }
      else System.out.println("接口未知的情况,code=" + code + "," + in + ",url:" + url)
    } catch {
      case e: Exception =>
        System.out.println("接口调用出现异常……")
    }
  }

}

