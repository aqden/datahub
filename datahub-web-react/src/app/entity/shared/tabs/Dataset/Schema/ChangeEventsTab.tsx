import React from 'react';
import { Table } from 'antd';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import { useGetDatasetChangeEventsQuery } from '../../../../../../graphql/datasetChangeEvents.generated';
import { ChangeEvent } from '../../../../../../types.generated';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';

export const ChangeEventsTab = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const datasetUrn: string = baseEntity?.dataset?.urn || '';

    // todo: query the change events from the past 1 month, past 1 year or all
    const { data: getDatasetChangeEventsData } = useGetDatasetChangeEventsQuery({
        skip: !datasetUrn,
        variables: {
            input: {
                datasetUrn,
            },
        },
    });

    const data: Array<ChangeEvent> = getDatasetChangeEventsData?.getDatasetChangeEvents?.changedEventsList || [];

    // to filter the events by actor
    const nameFilter = Array.from(new Set(data.map((obj) => obj.actor))).map((actor) => {
        return { text: actor, value: actor };
    });

    // to filter the events by category type
    const categoryFilter = Array.from(new Set(data.map((obj) => obj.category))).map((category) => {
        return { text: category, value: category };
    });

    const columns = [
        {
            title: 'Changed By',
            dataIndex: 'actor',
            filters: nameFilter,
            filterSearch: true,
        },
        {
            title: 'Datetime',
            dataIndex: 'timestampMillis',
            sorter: {
                compare: (a, b) => a.timestampMillis - b.timestampMillis,
                multiple: 3,
            },
            render: (timeStampMillis: number) => toLocalDateTimeString(timeStampMillis),
        },
        {
            title: 'Category',
            dataIndex: 'category',
            filters: categoryFilter,
            filterSearch: true,
        },
        {
            title: 'Change Operation',
            dataIndex: 'operation',
        },
        {
            title: 'Change Description',
            dataIndex: 'description',
        },
    ];

    return (
        <div>
            <Table columns={columns} dataSource={data} />
        </div>
    );
};
