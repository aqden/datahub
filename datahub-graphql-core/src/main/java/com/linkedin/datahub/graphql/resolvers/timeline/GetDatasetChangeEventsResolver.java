package com.linkedin.datahub.graphql.resolvers.timeline;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.timeline.mappers.SchemaBlameMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/*
Returns the most recent changes made to each column in a dataset at each dataset version.
 */
@Slf4j
public class GetDatasetChangeEventsResolver implements DataFetcher<CompletableFuture<GetDatasetChangeEventsResult>> {
  private final TimelineService _timelineService;

  public GetDatasetChangeEventsResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetDatasetChangeEventsResult> get(final DataFetchingEnvironment environment) throws Exception {
    final GetDatasetChangeEventsInput input = bindArgument(environment.getArgument("input"), GetDatasetChangeEventsInput.class);

    final String datasetUrnString = input.getDatasetUrn();
    final long startTime = 0;
    final long endTime = 0;

    return CompletableFuture.supplyAsync(() -> {
      try {
        final Set<ChangeCategory> changeCategorySet = new HashSet<>();
        // to include all change categories
        changeCategorySet.addAll(Arrays.asList(ChangeCategory.values()));
        Urn datasetUrn = Urn.createFromString(datasetUrnString);
        List<ChangeTransaction> changeTransactionList =
            _timelineService.getTimeline(datasetUrn, changeCategorySet, startTime, endTime, null, null, false);

        // mock 1 result
        // todo: need to use a mapper to set result
        GetDatasetChangeEventsResult result = new GetDatasetChangeEventsResult();
        result.setActor("test");
        result.setTimestampMillis(0L);
        ChangeEvent changeEvent = new ChangeEvent();
        changeEvent.setCategory(ChangeCategoryType.TECHNICAL_SCHEMA);
        changeEvent.setDescription("change description");
        changeEvent.setOperation(ChangeOperationType.ADD);
        List<ChangeEvent> list = new ArrayList<>();
        list.add(changeEvent);
        result.setChangeEventsList(list);
        return result;
      } catch (URISyntaxException u) {
        log.error(
            String.format("Failed to list schema blame data, likely due to the Urn %s being invalid", datasetUrnString),
            u);
        return null;
      } catch (Exception e) {
        log.error("Failed to list schema blame data", e);
        return null;
      }
    });
  }
}