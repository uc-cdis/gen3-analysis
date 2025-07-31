from unittest.mock import MagicMock, AsyncMock


def mock_guppy_data(app, data_list):
    """
    data_list: A list of data objects to return on successive calls
    """
    mocked_guppy_client = MagicMock()

    # AsyncMock is specifically designed for async functions
    mocked_execute_function = AsyncMock(side_effect=data_list)
    mocked_guppy_client.execute = mocked_execute_function
    app.state.guppy_client = mocked_guppy_client
