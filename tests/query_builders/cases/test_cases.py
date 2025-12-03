from unittest.mock import AsyncMock, ANY
import pytest
from gen3analysis.query_builders.cases.cases import get_item_ids
from unittest.mock import AsyncMock
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.cases.cases import case_summary_query


@pytest.mark.asyncio
async def test_get_item_ids_valid_data():
    mock_client = AsyncMock(spec=GuppyGQLClient)
    mock_client.execute.return_value = {
        "data": {"docType": [{"id1": "value1"}, {"id2": "value2"}]}
    }

    doc_type = "docType"
    item_fields = ["id1", "id2"]
    guppy_filter = {"some": "filter"}
    limit = 10
    access_token = "valid_token"

    result = await get_item_ids(
        mock_client,
        doc_type,
        item_fields,
        guppy_filter,
        limit=limit,
        access_token=access_token,
    )

    assert result == {"data": {"docType": [{"id1": "value1"}, {"id2": "value2"}]}}
    mock_client.execute.assert_awaited_once_with(
        access_token=access_token,
        query=ANY,
        variables={"filter": guppy_filter},
    )


@pytest.mark.asyncio
async def test_get_item_ids_no_results():
    mock_client = AsyncMock(spec=GuppyGQLClient)
    mock_client.execute.return_value = {"data": {"docType": []}}

    doc_type = "docType"
    item_fields = ["id1", "id2"]
    guppy_filter = {"some": "filter"}
    limit = 10
    access_token = "valid_token"

    result = await get_item_ids(
        mock_client,
        doc_type,
        item_fields,
        guppy_filter,
        limit=limit,
        access_token=access_token,
    )

    assert result == {"data": {"docType": []}}
    mock_client.execute.assert_awaited_once_with(
        access_token=access_token,
        query=ANY,
        variables={"filter": guppy_filter},
    )


@pytest.mark.asyncio
async def test_get_item_ids_invalid_token():
    mock_client = AsyncMock(spec=GuppyGQLClient)
    mock_client.execute.side_effect = Exception("Unauthorized")

    doc_type = "docType"
    item_fields = ["id1", "id2"]
    guppy_filter = {"some": "filter"}
    access_token = "invalid_token"

    with pytest.raises(Exception, match="Unauthorized"):
        await get_item_ids(
            mock_client,
            doc_type,
            item_fields,
            guppy_filter,
            access_token=access_token,
        )

    mock_client.execute.assert_awaited_once_with(
        access_token=access_token,
        query=ANY,
        variables={"filter": guppy_filter},
    )


@pytest.mark.asyncio
async def test_case_summary_query_success(mocker):
    # Mock the GuppyGQLClient
    mock_client = AsyncMock(spec=GuppyGQLClient)

    # Mock input
    test_case_id = "case123"
    test_access_token = "test_token"

    # Mock response data for the GraphQL client
    mock_response_data = {
        "data": {"caseSummary": [{"field_1": "value1", "field_2": "value2"}]}
    }

    mock_client.execute.return_value = mock_response_data

    # Call the function
    result = await case_summary_query(mock_client, test_case_id, test_access_token)

    # Assertions
    assert result == mock_response_data
    mock_client.execute.assert_called_once_with(
        access_token=test_access_token,
        query=mocker.ANY,  # Query string is dynamic
        variables={"filter": {"in": {"case_id": [test_case_id]}}},
    )


@pytest.mark.asyncio
async def test_case_summary_query_no_access_token(mocker):
    # Mock the GuppyGQLClient
    mock_client = AsyncMock(spec=GuppyGQLClient)

    # Mock input
    test_case_id = "case123"

    # Mock response data for the GraphQL client
    mock_response_data = {
        "data": {"caseSummary": [{"field_1": "value1", "field_2": "value2"}]}
    }

    mock_client.execute.return_value = mock_response_data

    # Call the function without providing an access token
    result = await case_summary_query(mock_client, test_case_id, access_token=None)

    # Assertions
    assert result == mock_response_data
    mock_client.execute.assert_called_once_with(
        access_token=None,
        query=mocker.ANY,  # Query string is dynamic
        variables={"filter": {"in": {"case_id": [test_case_id]}}},
    )


@pytest.mark.asyncio
async def test_case_summary_query_empty_response(mocker):
    # Mock the GuppyGQLClient
    mock_client = AsyncMock(spec=GuppyGQLClient)

    # Mock input
    test_case_id = "case123"
    test_access_token = "test_token"

    # Mock an empty response
    mock_response_data = {"data": {}}

    mock_client.execute.return_value = mock_response_data

    # Call the function
    result = await case_summary_query(mock_client, test_case_id, test_access_token)

    # Assertions
    assert result == mock_response_data
    mock_client.execute.assert_called_once_with(
        access_token=test_access_token,
        query=mocker.ANY,  # Query string is dynamic
        variables={"filter": {"in": {"case_id": [test_case_id]}}},
    )
