package cn.geo;



import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 此类中的方法只适用 Geo_point
 */
public class GeoPointDemo {
    private static String field = "location";

    private static GeoPoint center = new GeoPoint(40,10);

    private static String distance = "10000";

    private static List<GeoPoint> points =new ArrayList<>();

    private static GeoPoint topLeft = new GeoPoint();
    private static GeoPoint bottomRight =new GeoPoint();

    public static void main(String[] args) throws IOException {



        points.add(new GeoPoint(0,0));
        points.add(new GeoPoint(0,40));
        points.add(new GeoPoint(40,40));
        points.add(new GeoPoint(50,-42));
        points.add(new GeoPoint(-40,-50));
//        points.add(new GeoPoint(0,0));


        //多边形查询
//         PolygonSearch(field,points);
        //矩形查询
         RectangleSearch(field, topLeft, bottomRight);
        //圆形查询
//         DistanceSearch(field, distance, center);


    }


    /**
     * 矩形范围查找
     * @param field
     * @param topLeftPoint
     * @param bottomRightPoint
     * @return
     * @throws IOException
     */
    private static SearchResult RectangleSearch(String field,GeoPoint topLeftPoint,GeoPoint bottomRightPoint) throws IOException {
        RestHighLevelClient client = ESRestClientUtil.getDefaultClient();
        SearchResult result =new SearchResult();

        SearchRequest request = new SearchRequest();

        request.indices("my_location").types("_doc").source();

        SearchSourceBuilder builder = new SearchSourceBuilder();
        //矩形
        GeoBoundingBoxQueryBuilder geoBoundingBoxQueryBuilder  = QueryBuilders.geoBoundingBoxQuery(field)
                 .setCorners(topLeftPoint,bottomRightPoint);

        builder.query(geoBoundingBoxQueryBuilder);

        request.source(builder);


        long start = System.currentTimeMillis();
        SearchResponse response =  client.search(request);
        long end = System.currentTimeMillis();

        int searchTime = (int)(end-start);

        SearchHit[] hits= response.getHits().getHits();

        int totalHits =(int)response.getHits().getTotalHits();
        int tookTime = (int)response.getTook().getMillis();
        int length =  response.getHits().getHits().length;



        result.setTotalHits(totalHits);
        result.setSearchTime(searchTime);
        result.setTooK(tookTime);
        result.setResultCount(length);
        result.setThreadSize(0);

        for (SearchHit hit:hits) {
            System.out.println(hit.getSourceAsString());

        }
        client.close();
        return result;

    }


    /**
     * 查指定距离内点
     * @param field    :查询的字段
     * @param distance : 距离
     * @param center:   中心点
     * @return
     */
    private static SearchResult DistanceSearch(String field, String distance, GeoPoint center) throws IOException {
        RestHighLevelClient client = ESRestClientUtil.getDefaultClient();

        SearchSourceBuilder builder = new SearchSourceBuilder();
        SearchRequest request = new SearchRequest();
//        request.indices("my_location").types("_doc").source();

        SearchResult result = new SearchResult();

        //distance 查询
        GeoDistanceQueryBuilder geoDistanceQueryBuilder = QueryBuilders.geoDistanceQuery(field);
        geoDistanceQueryBuilder.point(center).distance(distance,DistanceUnit.KILOMETERS);

        builder.query(geoDistanceQueryBuilder);

//        request.source(builder);


//        builder.query(qb);
        request.source(builder);

        long start = System.currentTimeMillis();
        SearchResponse response = client.search(request);
        long end = System.currentTimeMillis();

        int searchTime = (int) (end - start);

        SearchHit[] hits = response.getHits().getHits();

        int totalHits = (int) response.getHits().getTotalHits();
        int tookTime = (int) response.getTook().getMillis();
        int length = response.getHits().getHits().length;


        result.setTotalHits(totalHits);
        result.setSearchTime(searchTime);
        result.setTooK(tookTime);
        result.setResultCount(length);
        result.setThreadSize(0);

        System.out.println(result);

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

        }
        client.close();
        return result;


    }


    /**
     *
     * @param Field : 查询字段
     * @param points : 构成多边形中的点,至少5个点
     * @return
     * @throws IOException
     */
    private static SearchResult PolygonSearch(String Field,List<GeoPoint> points) throws IOException {
        RestHighLevelClient client =ESRestClientUtil.getDefaultClient();

        SearchSourceBuilder builder = new SearchSourceBuilder();
        SearchRequest request = new SearchRequest();
        request.indices("my_location").types("_doc").source();

        SearchResult result =new SearchResult();

        //根据 clooection 中的点构成形状查询
         GeoPolygonQueryBuilder geoPolygonQueryBuilder  =  QueryBuilders.geoPolygonQuery(Field, points);

         builder.query(geoPolygonQueryBuilder);

         request.source(builder);


        long start = System.currentTimeMillis();
        SearchResponse response =  client.search(request);
        long end = System.currentTimeMillis();

        int searchTime = (int)(end-start);

         SearchHit[] hits= response.getHits().getHits();

        int totalHits =(int)response.getHits().getTotalHits();
        int tookTime = (int)response.getTook().getMillis();
        int length =  response.getHits().getHits().length;



        result.setTotalHits(totalHits);
        result.setSearchTime(searchTime);
        result.setTooK(tookTime);
        result.setResultCount(length);
        result.setThreadSize(0);

        for (SearchHit hit:hits) {
            System.out.println(hit.getSourceAsString());

        }
        client.close();
        return result;


    }

}