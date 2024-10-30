/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.operate.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.*;

import static io.camunda.operate.search.util.ElasticsearchUtil.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

@SpringBootApplication
public class Main {

  @Autowired
  private RestHighLevelClient esClient;

  private static Logger LOG = LoggerFactory.getLogger(Main.class);
  @Value("${camunda.operate.partitionId}")
  private int partitionId;
  @Value("${camunda.operate.importPosition}")
  private long importPosition;

  private String indices = "zeebe-record_process-instance_*";

  public static void main(String[] args) {
    LOG.info("STARTING THE APPLICATION");
    SpringApplication.run(Main.class, args);
    LOG.info("APPLICATION FINISHED");
  }

  public void start() throws Exception {
    searchMissingFlowNodeInstances();
    System.exit(0);
  }

  @Bean
  public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
    return args -> {
      this.start();
    };
  }

  private void searchMissingFlowNodeInstances() throws IOException {
    QueryBuilder query = joinWithAnd(
        termQuery("partitionId", partitionId),
        rangeQuery("position").gt(importPosition),
        scriptQuery(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
            "return doc['value.parentProcessInstanceKey']!=-1;", new HashMap<>())));
    CountRequest countRequest = new CountRequest(indices).query(query);
    System.out.println(String.format("Amount of records with parent on partition %s: %s", partitionId, esClient.count(countRequest, RequestOptions.DEFAULT).getCount()));

    SearchRequest request = new SearchRequest(indices)
        .source(new SearchSourceBuilder()
            .query(query)
            .sort("sequence", SortOrder.ASC)
            .fetchSource(new String[]{"key", "value.processInstanceKey", "value.parentElementInstanceKey"}, null)
            .size(3000));
    Set<Long> nonExistingParents = new HashSet<>();
    scrollWith(request, esClient, sh -> {
      Set<Long> elementInstanceKeys = collectElementInstanceKeys(sh);
      try {
        if (!elementInstanceKeys.isEmpty()) {
          nonExistingParents.addAll(searchForNonExistingParents(elementInstanceKeys));
          System.out.println("Non existing parents after current iteration: " + nonExistingParents);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    System.out.println("Non existing parents: " + nonExistingParents);
  }

  private Set<Long> searchForNonExistingParents(Set<Long> elementInstanceKeys) throws IOException {
    QueryBuilder query = joinWithAnd(
        termsQuery("key", elementInstanceKeys));
    SearchRequest request = new SearchRequest("operate*")
        .source(new SearchSourceBuilder()
            .query(query)
            .fetchSource(new String[]{"key"}, null)
            .size(1000));
    List<Long> existingParents = scrollFieldToList(request, "key", esClient);
    elementInstanceKeys.removeAll(existingParents);
    return elementInstanceKeys;
  }

  private Set<Long> collectElementInstanceKeys(SearchHits sh) {
    Set<Long> result = new HashSet<>();
    Arrays.stream(sh.getHits()).forEach(
        hit -> {
          try {
            result.add(((Map<String, Long>) hit.getSourceAsMap().get("value")).get("parentElementInstanceKey"));
          } catch (ClassCastException ex) {
            int i=1;
          }
        }
    );
    return result;
  }

}