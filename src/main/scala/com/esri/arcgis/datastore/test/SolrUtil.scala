package com.esri.arcgis.datastore.test

import com.esri.core.geometry._
import org.apache.commons.logging.LogFactory
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.solr.client.solrj.impl.HttpSolrClient
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree._

object SolrUtil {

  private val LOGGER  = LogFactory.getLog(SolrUtil.getClass.getName)

  def createExtentFilterForSolrQuery(geometryFieldName: String, geometryOption: Option[(String, String)]): String = {
    val geometry = createGeometry(geometryOption)
    val extentOption: Option[Envelope] = createExtentEnvelope(geometry)
    val filter = extentOption match {
      case Some(envelope) => // Solr arbitrary rectangle query
        s"$geometryFieldName:[${envelope.getYMin},${envelope.getXMin} TO ${envelope.getYMax},${envelope.getXMax}]"
      case _ => ""
    }
    filter
  }

  private def createGeometry(geometryOption: Option[(String, String)]): Option[Geometry] = {
    geometryOption match {
      case Some((geomType, geom)) =>
        geomType match {
          case "esriGeometryPoint" =>
            None // FIXME: to be implemented

          case "esriGeometryEnvelope" =>
            if (geom.trim.startsWith("{") && geom.trim.endsWith("}")) {
              val geomJson = GeometryEngine.jsonToGeometry(geom).getGeometry
              createExtentEnvelope(Option(geomJson))
            } else {
              val extent = geom.split(",")
              if (extent.length == 4) {
                Option(new Envelope(extent(0).toDouble, extent(1).toDouble, extent(2).toDouble, extent(3).toDouble))
              } else {
                None
              }
            }

          case "esriGeometryPolygon" =>
            if (geom.trim.startsWith("{") && geom.trim.endsWith("}")) {
              val geomJson = GeometryEngine.jsonToGeometry(geom).getGeometry
              createExtentEnvelope(Option(geomJson))
            } else {
              None // FIXME: to be implemented
            }

          case "esriGeometryPolyline" =>
            if (geom.trim.startsWith("{") && geom.trim.endsWith("}")) {
              val geomJson = GeometryEngine.jsonToGeometry(geom).getGeometry
              createExtentEnvelope(Option(geomJson))
            } else {
              None // FIXME: to be implemented
            }

          case "esriGeometryMultipoint" | _  =>
            None
        }
      case _ => None
    }
  }

  private def createExtentEnvelope(spatialGeometry: Option[Geometry]): Option[Envelope] = {
    spatialGeometry match {
      case Some(extent: Envelope) =>
        Option(extent)
      case Some(area: Polygon) =>
        val envelope = new Envelope
        area.queryEnvelope(envelope)
        Option(envelope)
      case _ => None
    }
  }

  def createFilterQueryStringForWhereClause(where: String): Option[String] = {
    val sql = "SELECT * FROM dummy WHERE " + where
    try {
      val statement = new SqlParser().createStatement(sql).asInstanceOf[Query]
      val queryBody = statement.getQueryBody.asInstanceOf[QuerySpecification]
      val whereClause = queryBody.getWhere
      val whereExpr = whereClause.get()
      return Option("(" + createFQString(whereExpr) + ")")
    } catch {
      case _: Throwable =>
        LOGGER.warn("Invalid where clause -> " + where)
    }
    None
  }

  private def createFQString(expr: Expression): String = {
    expr match {
      case lb_expr: LogicalBinaryExpression =>
        createFQForLogicBinaryExpression(lb_expr)
      case comp_expr: ComparisonExpression =>
        createFQForComparisonExpression(comp_expr)
      case btp: BetweenPredicate =>
        createFQForBetweenPredicate(expr.asInstanceOf[BetweenPredicate])
      case _ =>
        ""
    }
  }

  private def createFQForLogicBinaryExpression(expr: LogicalBinaryExpression): String = {
    "(" + createFQString(expr.getLeft) + ") " + expr.getType + " (" + createFQString(expr.getRight) + ")"
  }

  private def createFQForComparisonExpression(expr: ComparisonExpression): String = {
    val left = expr.getLeft
    val right = expr.getRight
    val op = expr.getType

    val supported = left.isInstanceOf[Identifier] & right.isInstanceOf[Literal]
    if (!supported)
      throw new RuntimeException("Unsupported expression ->"  +  expr.toString)

    val fieldName = removeDoubleQuotes(left.toString)
    val fieldValue = removeSingleQuotes(right.toString)
    op match {
      case ComparisonExpressionType.EQUAL =>
        fieldName + ":" + fieldValue
      case ComparisonExpressionType.GREATER_THAN =>
        fieldName + ":[" + fieldValue + " *] AND !" + fieldName +":" + fieldValue
      case ComparisonExpressionType.GREATER_THAN_OR_EQUAL =>
        fieldName + ":[" + fieldValue + " *]"
      case ComparisonExpressionType.LESS_THAN =>
        fieldName + ":[* " + fieldValue + "] AND !" + fieldName +":" + fieldValue
      case ComparisonExpressionType.LESS_THAN_OR_EQUAL =>
        fieldName + ":[* " + fieldValue + "]"
      case ComparisonExpressionType.NOT_EQUAL =>
        "!" + fieldName +":" + fieldValue
      case _ =>
        throw new RuntimeException("Unsupported expression ->"  +  expr.toString)
    }
  }

  private def createFQForBetweenPredicate(expr: BetweenPredicate): String = {
    removeDoubleQuotes(expr.getValue.toString) +":[" + expr.getMin.toString + " " + expr.getMax.toString+"]"
  }

  // field name shouldn't use double quote as part of its name
  private def removeDoubleQuotes(item: String): String = {
    item.replaceAll("\"", "")
  }

  // remove only the starting and ending single quotes
  private def removeSingleQuotes(item: String): String = {
    var removed = if (item.startsWith("'")) item.substring(1) else item
    removed = if (removed.endsWith("'")) removed.substring(0, removed.length-1) else removed
    removed
  }


  def getSolrClient(hostName: String, port: Int, keyspace: String, tableName: String): SolrClient = {
    val baseUrl = "http://" + hostName + ":" + port + "/solr/" + keyspace + "." + tableName
    new HttpSolrClient(baseUrl)
  }

  def main(args: Array[String]): Unit = {
    val solrQuery = new SolrQuery()
    solrQuery.set("fq", "(orig:(\"N'Djamena International Airport\"))")
    println(solrQuery.toString)
  }
}
