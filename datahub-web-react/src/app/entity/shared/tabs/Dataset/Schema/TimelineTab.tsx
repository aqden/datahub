import React from 'react';
import { Table } from 'antd';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import { useGetDatasetChangeEventsQuery } from '../../../../../../graphql/datasetChangeEvents.generated';
import { ChangeEvent } from '../../../../../../types.generated';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';

export const TimelineTab = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const datasetUrn: string = baseEntity?.dataset?.urn || '';

    const { data: getDatasetChangeEventsData } = useGetDatasetChangeEventsQuery({
        skip: !datasetUrn,
        variables: {
            input: {
                datasetUrn,
            },
        },
    });

    const data: Array<ChangeEvent> = getDatasetChangeEventsData?.getDatasetChangeEvents?.changedEventsList || [];

    const columns = [
        {
            title: 'Edited By',
            dataIndex: 'actor',
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
        },
        {
            title: 'Operation',
            dataIndex: 'operation',
        },
        {
            title: 'Description',
            dataIndex: 'description',
        },
    ];

    return (
        <div>
            <Table columns={columns} dataSource={data} />
        </div>
    );
};
