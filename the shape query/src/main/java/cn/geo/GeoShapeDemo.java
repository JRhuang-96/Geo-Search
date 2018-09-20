package cn.geo;


import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 此类中的方法只适用 Geo_Shape
 */
public class GeoShapeDemo {

    private static List<Coordinate> lintString = new ArrayList<>();
    private static String field = "location";
    private static List<Coordinate> polyList = new ArrayList<>();
    private static CoordinatesBuilder coordinatesBuilder =new CoordinatesBuilder();
    private static Coordinate center = new Coordinate(0,0);
    private static String radius = "10000";
    private static Coordinate topLeft = new Coordinate(96.676839,52.444825);
    private static Coordinate bottomRight =new Coordinate(113.398215,21.217703);

    public static void main(String[] args) throws IOException {

    //线的点
//        lintString.add(new Coordinate(0,0));
//        lintString.add(new Coordinate(100,90 ));
    //第二条线
//        lintString.add(new Coordinate(86.286115, 31.209333));
//        lintString.add(new Coordinate(20.973503,20.730249 ));
//    第三条
        lintString.add(new Coordinate(96.676839,52.444825 ));
        lintString.add(new Coordinate(113.398215,21.217703 ));

    //多边形的点
        polyList.add(new Coordinate(40, 60));
        polyList.add(new Coordinate(100, 60));
        polyList.add(new Coordinate(100, 40 ));
        polyList.add(new Coordinate(20, 40 ));
        polyList.add(new Coordinate(40, 60));

//        polyList.add(new Coordinate(90.512667,23.050805 ));
//        polyList.add(new Coordinate( 113.398215,21.217703));




        //矩形查询
//        RectangleSearch(field,topLeft,bottomRight);

        //多点查询
//        coordinatesBuilder.coordinates(polyList);
//        MultiPonitSearch(field,polyList);

        //线查询
//        LineStringSearch(field,lintString);

        //多边形查询
//        PolygonStringSearch(field,coordinatesBuilder);

        //圆形查询
        DistanceSearch(field,center,radius);


        //多种类型一起
        //new GeometryCollection();

    }








    /**
     *矩形范围查找
     * @param field
     * @param topLeft
     * @param bottomRight
     * @return
     * @throws IOException
     */
    private static SearchResult  RectangleSearch(String field, Coordinate topLeft, Coordinate bottomRight) throws IOException {
        SearchResult result =new SearchResult();
        RestHighLevelClient client= ESRestClientUtil.getDefaultClient();

        SearchRequest request =new SearchRequest();
//        request.indices("analysys-tag-geo-shape-1").types("sorted-tag");
//        request.indices("example").types("doc");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();


        EnvelopeBuilder   envelopeBuilder = new EnvelopeBuilder(topLeft,bottomRight);


        QueryBuilder builder = new GeoShapeQueryBuilder(field,envelopeBuilder);


        sourceBuilder.query(builder);
        request.source(sourceBuilder);

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
        System.out.println();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
        return result;
    }


    /**
     * 多点匹配
     * 如果 multipoint 中包含需要匹配的点,会将改multipoint 一起返回
     * @param field : 查询字段
     * @param points : 查询的点
     * @return
     * @throws IOException
     */
    private static SearchResult  MultiPonitSearch(String field, List<Coordinate> points) throws IOException {
        SearchResult result =new SearchResult();
        RestHighLevelClient client= ESRestClientUtil.getDefaultClient();

        SearchRequest request =new SearchRequest();
//        request.indices("analysys-tag-geo-shape-1").types("sorted-tag");
//        request.indices("example").types("doc");

        SearchSourceBuilder sourceBuilder =new SearchSourceBuilder();
        MultiPointBuilder multiPointBuilder = new MultiPointBuilder(points);

        QueryBuilder builder = new GeoShapeQueryBuilder(field,multiPointBuilder);


        sourceBuilder.query(builder);
        request.source(sourceBuilder);

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
        System.out.println();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
        return result;
    }


    /**
     * 圆形范围查找
     如果 multipoint 中包含该一点在 Distance的范围内,会将该 multipoint一起返回
     * @param field : 查询字段
     * @param center : 圆点
     * @param radius : 半径
     * @return
     * @throws IOException
     */
    private static SearchResult  DistanceSearch(String field, Coordinate center,String radius) throws IOException {
        SearchResult result =new SearchResult();
        RestHighLevelClient client= ESRestClientUtil.getDefaultClient();

        SearchRequest request =new SearchRequest();
        request.indices("analysys-tag-geo-shape-1").types("sorted-tag");
//        request.indices("example").types("doc");

        SearchSourceBuilder sourceBuilder =new SearchSourceBuilder();

        CircleBuilder circle= new CircleBuilder();

        circle.center(center).radius(radius);

        QueryBuilder builder = new GeoShapeQueryBuilder(field,circle)
                .relation(ShapeRelation.CONTAINS);//选择图形之间的关系

        sourceBuilder.query(builder);
        request.source(sourceBuilder);

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
        System.out.println();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        client.close();
        return result;
    }


    /**
     * 多边形查询，至少五个点。选择图形之间的关系
     * @param field : 查询字段
     * @param polyList : 点成的集合
     * @return
     * @throws IOException
     */
    private static SearchResult  PolygonStringSearch(String field, CoordinatesBuilder polyList) throws IOException {
        SearchResult result =new SearchResult();
        RestHighLevelClient client= ESRestClientUtil.getDefaultClient();

        SearchRequest request =new SearchRequest();
//        request.indices("analysys-tag-geo-shape-1").types("sorted-tag");
//        request.indices("example-3").types("doc");

        SearchSourceBuilder sourceBuilder =new SearchSourceBuilder();

        PolygonBuilder  polygonBuilder  = new PolygonBuilder(polyList);

        QueryBuilder builder = new GeoShapeQueryBuilder(field,polygonBuilder)
                .relation(ShapeRelation.CONTAINS);//选择图形之间的关系

        sourceBuilder.query(builder);
        request.source(sourceBuilder);

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
        System.out.println();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

        }
        client.close();
        return result;
    }


    /**
     *
     *根据选择 与图形之间不同的关系,返回不同的结果集
     * @param field : 查询字段
     * @param lintString : 线
     * @return
     * @throws IOException
     */
    private static SearchResult  LineStringSearch(String field, List<Coordinate> lintString) throws IOException {
        SearchResult result =new SearchResult();
        RestHighLevelClient client= ESRestClientUtil.getDefaultClient();

        SearchRequest request =new SearchRequest();
        request.indices("analysys-tag-geo-shape-1").types("sorted-tag");
//        request.indices("example").types("doc");
        SearchSourceBuilder sourceBuilder =new SearchSourceBuilder();

        ShapeBuilder lineStringShape  =new LineStringBuilder(lintString);

        QueryBuilder builder = new GeoShapeQueryBuilder(field,lineStringShape)
                                    .relation(ShapeRelation.INTERSECTS);//选择图形之间的关系

        sourceBuilder.query(builder);
        request.source(sourceBuilder);

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
        System.out.println();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

        }
        client.close();
        return result;
    }
}
