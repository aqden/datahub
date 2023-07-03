import React from 'react';
import axios from 'axios';
import { render, waitFor, fireEvent } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { HomePage } from '../HomePage';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';

jest.mock('axios');

describe('HomePage', () => {
    const mockResponse = {
        message: 'hello world',
        timestamp: 1000000000,
    };

    it('renders with banner', async () => {
        const mockPost: jest.SpyInstance = jest.spyOn(axios, 'get');
        mockPost.mockImplementation(() =>
            Promise.resolve({
                data: mockResponse,
            }),
        );
        const { getByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(axios.get).toBeCalledTimes(1);
        await waitFor(() => expect(getByText('hello world')).toBeInTheDocument());
    });

    it('renders', async () => {
        const mockPost: jest.SpyInstance = jest.spyOn(axios, 'get');
        mockPost.mockImplementation(() =>
            Promise.resolve({
                data: mockResponse,
            }),
        );
        const { getByTestId } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByTestId('search-input')).toBeInTheDocument());
    });

    it('renders browsable entities', async () => {
        const mockPost: jest.SpyInstance = jest.spyOn(axios, 'get');
        mockPost.mockImplementation(() =>
            Promise.resolve({
                data: mockResponse,
            }),
        );
        const { getByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Datasets')).toBeInTheDocument());
    });

    it('renders autocomplete results', async () => {
        const mockPost: jest.SpyInstance = jest.spyOn(axios, 'get');
        mockPost.mockImplementation(() =>
            Promise.resolve({
                data: mockResponse,
            }),
        );
        const { getByTestId, queryAllByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        const searchInput = getByTestId('search-input');
        await waitFor(() => expect(searchInput).toBeInTheDocument());
        fireEvent.change(searchInput, { target: { value: 't' } });

        await waitFor(() => expect(queryAllByText('he Great Test Dataset').length).toBeGreaterThanOrEqual(1));
        expect(queryAllByText('Some Other Dataset').length).toBeGreaterThanOrEqual(1);
    });

    it('renders search suggestions', async () => {
        const mockPost: jest.SpyInstance = jest.spyOn(axios, 'get');
        mockPost.mockImplementation(() =>
            Promise.resolve({
                data: mockResponse,
            }),
        );
        const { getByText, queryAllByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Try searching for')).toBeInTheDocument());
        expect(queryAllByText('Yet Another Dataset').length).toBeGreaterThanOrEqual(1);
        expect(queryAllByText('Fourth Test Dataset').length).toBeGreaterThanOrEqual(1);
    });
});
