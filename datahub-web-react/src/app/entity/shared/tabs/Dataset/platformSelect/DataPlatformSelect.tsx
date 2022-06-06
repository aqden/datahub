import React, { useState } from 'react';
// import { Select } from 'antd';
import Select from 'antd/lib/select';
import styled from 'styled-components';
import { Form } from 'antd';
import { SetParentContainer } from '../containerEdit/SetParentContainer';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const SearchResultDisplayName = styled.div`
    margin-left: 5px;
`;

const formItemLayout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 14 },
};

const platformSelection = [
    'urn:li:dataPlatform:file',
    'urn:li:dataPlatform:mongodb',
    'urn:li:dataPlatform:elasticsearch',
    'urn:li:dataPlatform:mssql',
    'urn:li:dataPlatform:mysql',
    'urn:li:dataPlatform:oracle',
    'urn:li:dataPlatform:mariadb',
    'urn:li:dataPlatform:hdfs',
    'urn:li:dataPlatform:hive',
    'urn:li:dataPlatform:postgres',
];

export const DataPlatformSelect = () => {
    // Unfortunately there is no way to query for available platforms.
    // Hence, must resort to fixed list.
    const [selectedPlatform, setSelectedPlatform] = useState('');
    const renderSearchResult = (result: string) => {
        const displayName = result.split(':').pop()?.toUpperCase();
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <SearchResultDisplayName>
                        <div>{displayName}</div>
                    </SearchResultDisplayName>
                </SearchResultContent>
            </SearchResultContainer>
        );
    };

    const onSelectMember = (urn: string) => {
        setSelectedPlatform(urn);
    };
    console.log(`selected platform: ${selectedPlatform}`);
    const removeOption = () => {
        console.log(`removing ${selectedPlatform}`);
        setSelectedPlatform('');
    };
    return (
        <>
            <Form.Item
                {...formItemLayout}
                name="platformSelect"
                label="Specify a Data Source Type"
                rules={[
                    {
                        required: true,
                        message: 'A type MUST be specified.',
                    },
                ]}
            >
                <Select
                    style={{ width: 300 }}
                    autoFocus
                    filterOption={false}
                    value={selectedPlatform}
                    showArrow
                    placeholder="Search for a parent container.."
                    onSelect={(platform: string) => onSelectMember(platform)}
                    allowClear
                    onClear={removeOption}
                    onDeselect={removeOption}
                >
                    {platformSelection?.map((platform) => (
                        <Select.Option value={platform}>{renderSearchResult(platform)}</Select.Option>
                    ))}
                </Select>
            </Form.Item>
            <SetParentContainer platformType={selectedPlatform} compulsory={false} />
        </>
    );
};
