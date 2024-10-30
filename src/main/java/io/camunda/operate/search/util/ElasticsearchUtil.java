/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.operate.search.util;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public abstract class ElasticsearchUtil {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchUtil.class);

  public static final int SCROLL_KEEP_ALIVE_MS = 60000;

  /**
   * Join queries with AND clause. If 0 queries are passed for wrapping, then null is returned. If 1 parameter is passed, it will be returned back as ia. Otherwise, the new
   * BoolQuery will be created and returned.
   * @param queries
   * @return
   */
  public static QueryBuilder joinWithAnd(QueryBuilder... queries) {
    List<QueryBuilder> notNullQueries = throwAwayNullElements(queries);
    switch (notNullQueries.size()) {
    case 0:
      return null;
    case 1:
      return notNullQueries.get(0);
    default:
      final BoolQueryBuilder boolQ = boolQuery();
      for (QueryBuilder query: notNullQueries) {
        boolQ.must(query);
      }
      return boolQ;
    }
  }

  public static <T> List<T> throwAwayNullElements(T... array) {
    List<T> listOfNotNulls = new ArrayList<>();
    for (T o: array) {
      if (o != null) {
        listOfNotNulls.add(o);
      }
    }
    return listOfNotNulls;
  }

  public static void scrollWith(SearchRequest searchRequest, RestHighLevelClient esClient,
      Consumer<SearchHits> searchHitsProcessor) throws IOException {
    scrollWith(searchRequest, esClient, searchHitsProcessor,
        null, null);
  }

  public static void scrollWith(SearchRequest searchRequest, RestHighLevelClient esClient,
      Consumer<SearchHits> searchHitsProcessor, Consumer<Aggregations> aggsProcessor,
      Consumer<SearchHits> firstResponseConsumer) throws IOException {

    searchRequest.scroll(TimeValue.timeValueMillis(SCROLL_KEEP_ALIVE_MS));
    SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);

    if (firstResponseConsumer != null) {
      firstResponseConsumer.accept(response.getHits());
    }

    //call aggregations processor
    if (aggsProcessor != null) {
      aggsProcessor.accept(response.getAggregations());
    }

    String scrollId = response.getScrollId();
    SearchHits hits = response.getHits();
    while (hits.getHits().length != 0) {
      //call response processor
      if (searchHitsProcessor != null) {
        searchHitsProcessor.accept(response.getHits());
      }

      SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
      scrollRequest.scroll(TimeValue.timeValueMillis(SCROLL_KEEP_ALIVE_MS));

      response = esClient
          .scroll(scrollRequest, RequestOptions.DEFAULT);

      scrollId = response.getScrollId();
      hits = response.getHits();
    }

    clearScroll(scrollId, esClient);
  }

  public static <T> List<T> scrollFieldToList(SearchRequest request, String fieldName, RestHighLevelClient esClient) throws IOException {
    List<T> result = new ArrayList<>();
    Function<SearchHit, T> searchHitFieldToString = (searchHit) -> (T)searchHit.getSourceAsMap().get(fieldName);

    Consumer<SearchHits> collectFields = (hits) -> {
      result.addAll(map(hits.getHits(), searchHitFieldToString));
    };

    scrollWith(request, esClient, collectFields,null, collectFields);
    return result;
  }

  public static <S,T> List<T> map(S[] sourceArray, Function<S, T> mapper) {
    return map(Arrays.stream(sourceArray).parallel(), mapper);
  }

  public static <S,T> List<T> map(Stream<S> sequenceStream, Function<? super S, T> mapper) {
    return sequenceStream.map(mapper).collect(Collectors.toList());
  }

  public static void clearScroll(String scrollId, RestHighLevelClient esClient) {
    if (scrollId != null) {
      //clear the scroll
      ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.addScrollId(scrollId);
      try {
        esClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
      } catch (Exception e) {
        logger.warn("Error occurred when clearing the scroll with id [{}]", scrollId);
      }
    }
  }

}
