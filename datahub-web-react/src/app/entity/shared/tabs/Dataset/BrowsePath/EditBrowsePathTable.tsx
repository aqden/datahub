// import { Empty } from 'antd';
import React, { useEffect, useState } from 'react';
import { gql, useQuery } from '@apollo/client';
import { Button, Form, message } from 'antd';
import axios from 'axios';
import { GetDatasetOwnersSpecialQuery, GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import { SpecifyBrowsePath } from '../../../../../create/Components/SpecifyBrowsePath';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';

function computeFinal(input) {
    console.log('input');
    console.log(input);
    const dataPaths = input?.browsePaths.map((x) => {
        const temp: [] = x.path;
        temp.splice(temp.length - 1);
        // console.log(temp);
        return `/${temp.join('/')}/`;
        // return '/'+`${temp.join('/')}`+'/'
    });
    const formatted = dataPaths?.map((x) => {
        return x;
    });
    return formatted || [''];
}
function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

export const EditBrowsePathTable = () => {
    const [originalData, setOriginalData] = useState();
    const [disabledSave, setDisabledSave] = useState(true);
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currUrn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;
    const currDataset = useBaseEntity<GetDatasetOwnersSpecialQuery>()?.dataset?.urn;
    const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
    // console.log(currUrn);
    const [form] = Form.useForm();
    const queryresult = gql`
        {
            browsePaths(
                input: {
                    urn: "${currUrn}"
                    type: DATASET
                }
            ) {
                path
            }
        }
    `;
    const { data } = useQuery(queryresult, { skip: currUrn === undefined });
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };
    const onFinish = async (values) => {
        axios
            .post('http://localhost:8001/update_browsepath', {
                dataset_name: currDataset,
                requestor: currUser,
                browsePaths: values.browsepathList,
            })
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
        await timeout(3000);
        window.location.reload();
    };
    const onReset = () => {
        form.resetFields();
        form.setFieldsValue({
            browsepathList: originalData,
        });
    };
    const handleFormChange = () => {
        const hasErrors = form.getFieldsError().some(({ errors }) => errors.length);
        console.log(`changed value ${form.getFieldsValue()}`);
        setDisabledSave(hasErrors);
    };
    useEffect(() => {
        const formatted = computeFinal(data);
        setOriginalData(formatted);
        console.log(`formatted is ${formatted}`);
        form.resetFields();
        form.setFieldsValue({
            browsepathList: formatted,
        });
    }, [form, data]);
    return (
        <Form name="dynamic_item" {...layout} form={form} onFinish={onFinish} onFieldsChange={handleFormChange}>
            <Button type="primary" htmlType="submit" disabled={disabledSave}>
                Submit
            </Button>
            &nbsp;
            <Button htmlType="button" onClick={onReset}>
                Reset
            </Button>
            <SpecifyBrowsePath />
        </Form>
    );
};
