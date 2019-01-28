package com.esri.arcgis.dse.test;

import com.esri.core.geometry.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;

import java.util.Optional;

public class SolrUtil {

  private static Log LOGGER  = LogFactory.getLog(SolrUtil.class.getName());

  public static String createExtentFilterForSolrQuery(String geometryFieldName, String geometryType, String geometryString)  {
    Geometry geometry = createGeometry(geometryType, geometryString);
    Envelope envelope = createExtentEnvelope(geometry);
    String filter = envelope != null ?
        geometryFieldName + ":[" + envelope.getYMin() + "," + envelope.getXMin() + " TO " + envelope.getYMax() + "," + envelope.getXMax() : "";
    return filter;
  }

  private static Geometry createGeometry(String geomType, String geomString) {
    if (geomType != null && geomString != null) {
      if (geomType == "esriGeometryPoint") {
        return null; // FIXME: to be implemented
      }
      else if (geomType == "esriGeometryEnvelope") {
        if (geomString.trim().startsWith("{") && geomString.trim().endsWith("}")) {
          Geometry geomJson = GeometryEngine.jsonToGeometry(geomString).getGeometry();
          return createExtentEnvelope(geomJson);
        } else {
          String[] extent = geomString.split(",");
          if (extent.length == 4) {
            return new Envelope(Double.parseDouble(extent[0]), Double.parseDouble(extent[1]),
                Double.parseDouble(extent[2]), Double.parseDouble(extent[3]));
          } else {
            return null; // FIXME: to be implemented
          }
        }
      }
      else if (geomType == "esriGeometryPolygon") {
        if (geomString.trim().startsWith("{") && geomString.trim().endsWith("}")) {
          Geometry geomJson = GeometryEngine.jsonToGeometry(geomString).getGeometry();
          return createExtentEnvelope(geomJson);
        } else {
          return null; // FIXME: to be implemented
        }
      }
      else if (geomType == "esriGeometryPolyline") {
        if (geomString.trim().startsWith("{") && geomString.trim().endsWith("}")) {
          Geometry geomJson = GeometryEngine.jsonToGeometry(geomString).getGeometry();
          return createExtentEnvelope(geomJson);
        } else {
          return null; // FIXME: to be implemented
        }
      }
      else {
        return null; // FIXME: to be implemented
      }
    } else {
      return null;
    }
  }

  private static Envelope createExtentEnvelope(Geometry spatialGeometry) {
    if (spatialGeometry == null) {
      return null;
    }

    if (spatialGeometry instanceof Envelope) {
      return (Envelope)spatialGeometry;
    }
    else if (spatialGeometry instanceof  Polygon) {
      Envelope envelope = new Envelope();
      spatialGeometry.queryEnvelope(envelope);
      return envelope;
    }
    else
      return null;
  }

  public static String createFilterQueryStringForWhereClause(String where) {
    String sql = "SELECT * FROM dummy WHERE " + where;
    try {
      Query statement = (Query)(new SqlParser().createStatement(sql));
      QuerySpecification queryBody = (QuerySpecification)statement.getQueryBody();
      Optional<Expression> whereClause = queryBody.getWhere();
      Expression whereExpr = whereClause.get();
      return "(" + createFQString(whereExpr) + ")";
    } catch (Exception ex){
        LOGGER.warn("Invalid where clause -> " + where);
    }
    return null;
  }

  private static String createFQString(Expression expr) {
    if (expr instanceof LogicalBinaryExpression) {
      return createFQForLogicBinaryExpression((LogicalBinaryExpression)expr);
    }
    else if (expr instanceof ComparisonExpression) {
      return createFQForComparisonExpression((ComparisonExpression)expr);
    }
    else if (expr instanceof BetweenPredicate) {
      return createFQForBetweenPredicate((BetweenPredicate)expr);
    }
    else {
      return "";
    }
  }

  private static String createFQForLogicBinaryExpression(LogicalBinaryExpression expr) {
    return "(" + createFQString(expr.getLeft()) + ") " + expr.getType() + " (" + createFQString(expr.getRight()) + ")";
  }

  private static String createFQForComparisonExpression(ComparisonExpression expr) {
    Expression left = expr.getLeft();
    Expression right = expr.getRight();
    ComparisonExpressionType op = expr.getType();

    boolean supported = (left instanceof Identifier) & (right instanceof Literal);
    if (!supported)
      throw new RuntimeException("Unsupported expression ->"  +  expr.toString());

    String fieldName = removeDoubleQuotes(left.toString());
    String fieldValue = removeSingleQuotes(right.toString());
    if (op == ComparisonExpressionType.EQUAL) {
      return fieldName + ":" + fieldValue;
    } else if (op == ComparisonExpressionType.GREATER_THAN) {
      return fieldName + ":[" + fieldValue + " *] AND !" + fieldName + ":" + fieldValue;
    } else if (op == ComparisonExpressionType.GREATER_THAN_OR_EQUAL) {
      return fieldName + ":[" + fieldValue + " *]";
    } else if (op == ComparisonExpressionType.LESS_THAN) {
      return fieldName + ":[* " + fieldValue + "] AND !" + fieldName + ":" + fieldValue;
    } else if (op == ComparisonExpressionType.LESS_THAN_OR_EQUAL) {
      return fieldName + ":[* " + fieldValue + "]";
    } else if (op == ComparisonExpressionType.NOT_EQUAL) {
      return "!" + fieldName + ":" + fieldValue;
    } else {
        throw new RuntimeException("Unsupported expression ->"  +  expr.toString());
    }
  }

  private static String createFQForBetweenPredicate(BetweenPredicate expr) {
    return removeDoubleQuotes(expr.getValue().toString()) +":[" + expr.getMin().toString() + " " + expr.getMax().toString()+"]";
  }

  // field name shouldn't use double quote as part of its name
  private static String removeDoubleQuotes(String item) {
    return item.replaceAll("\"", "");
  }

  // remove only the starting and ending single quotes
  private static String removeSingleQuotes(String item) {
    String removed = (item.startsWith("'")) ? item.substring(1) : item;
    removed = (removed.endsWith("'")) ? removed.substring(0, removed.length()-1) : removed;
    return removed;
  }

  public static SolrClient getSolrClient(String hostName, int port, String keyspace, String tableName)  {
    String baseUrl = "http://" + hostName + ":" + port + "/solr/" + keyspace + "." + tableName;
    return new HttpSolrClient(baseUrl);
  }

}
