package cn.geo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class PointAgg {
    public static void main(String[] args) throws IOException {
      GeoPoint point =   new GeoPoint(20,70);
      String field ="location";
//        DistanceAgg(field,point);
        HashGridAgg(field);





    }


    private static void HashGridAgg(String field) throws IOException {
        RestHighLevelClient client = ESRestClientUtil.getDefaultClient();
        SearchRequest request = new SearchRequest();
        request.indices("my_location").types("_doc").source();

        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder =  AggregationBuilders.geohashGrid("agg")
                                                                    .field(field)
                                                                    .precision(4);


        builder.aggregation(aggregationBuilder);
        request.source(builder);

        SearchResponse response= client.search(request);

        GeoHashGrid agg = response.getAggregations().get("agg");
        for(GeoHashGrid.Bucket entry :agg.getBuckets()){
            String keyString  = entry.getKeyAsString();  // key
            GeoPoint key = (GeoPoint)entry.getKey(); //bucket from value
            long docCount = entry.getDocCount();
            System.out.println("key [{"+keyString+"}],point [{"+key+"}], doc_count [{"+docCount+"}]");
        }


        client.close();


    }


    private static void DistanceAgg(String field, GeoPoint point) throws IOException {
        RestHighLevelClient client = ESRestClientUtil.getDefaultClient();
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder builder = new SearchSourceBuilder();

        GeoDistanceAggregationBuilder geo =  AggregationBuilders.geoDistance("agg", point)
                                                                .field(field)
                                                                .unit(DistanceUnit.KILOMETERS)
                                                                .addUnboundedTo(3.0)
                                                                .addRange(3.0,10)
                                                                .addRange(10.0,500);


        builder.aggregation(geo);
        request.source(builder);

        SearchResponse response= client.search(request);

        Range agg = response.getAggregations().get("agg");
        for(Range.Bucket entry :agg.getBuckets()){
            String key = entry.getKeyAsString();  // key
            Number from = (Number)entry.getFrom(); //bucket from value
            Number to = (Number)entry.getTo();
            long docCount = entry.getDocCount();
            System.out.println("key [{"+key+"}], from [{"+from+"}], to [{"+to+"}], doc_count [{"+docCount+"}]");
        }


        client.close();
    }


}
