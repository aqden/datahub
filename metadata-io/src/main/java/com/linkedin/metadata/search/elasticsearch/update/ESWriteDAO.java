package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;


@Slf4j
@RequiredArgsConstructor
public class ESWriteDAO {

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient searchClient;
  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;

  /**
   * Updates or inserts the given search document.
   *
   * @param entityName name of the entity
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    final UpdateRequest updateRequest = new UpdateRequest(
            indexName, docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(numRetries);

    bulkProcessor.add(updateRequest);
  }

  /**
   * Creates a request to insert new document into datahub_update_event index
   *
   * @param document the document to insert
   */
  public void createUpdateDocument(@Nonnull String document) {
    final String indexName = indexConvention.getIndexName("datahub_update_event");
    final IndexRequest indexRequest = new IndexRequest(
        indexName)
        .source(document, XContentType.JSON)
        .opType(DocWriteRequest.OpType.CREATE);

    bulkProcessor.add(indexRequest);
  }

  /**
   * Deletes the document with the given document ID from the index.
   *
   * @param entityName name of the entity
   * @param docId the ID of the document to delete
   */
  public void deleteDocument(@Nonnull String entityName, @Nonnull String docId) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    bulkProcessor.add(new DeleteRequest(indexName).id(docId));
  }

  /**
   * Applies a script to a particular document
   */
  public void applyScriptUpdate(@Nonnull String entityName, @Nonnull String docId, @Nonnull String script) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    UpdateRequest updateRequest = new UpdateRequest(indexName, docId)
            .detectNoop(false)
            .scriptedUpsert(true)
            .retryOnConflict(numRetries)
            .script(new Script(script));
    bulkProcessor.add(updateRequest);
  }

  /**
   * Clear all documents in all the indices
   */
  public void clear() {
    String[] indices = getIndices(indexConvention.getAllEntityIndicesPattern());
    bulkProcessor.deleteByQuery(QueryBuilders.matchAllQuery(), indices);
  }

  private String[] getIndices(String pattern) {
    try {
      GetIndexResponse response = searchClient.indices().get(new GetIndexRequest(pattern), RequestOptions.DEFAULT);
      return response.getIndices();
    } catch (IOException e) {
      log.error("Failed to get indices using pattern {}", pattern);
      return new String[]{};
    }
  }
}
