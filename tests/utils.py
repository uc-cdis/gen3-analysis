from unittest.mock import MagicMock


def mock_guppy_data(app, data):
    async def mocked_guppy_data():
        return data

    mocked_guppy_client = MagicMock()
    # making this function a MagicMock allows us to use methods like
    # `assert_called_once_with` in the tests
    mocked_execute_function = MagicMock(
        side_effect=lambda *args, **kwargs: (
            await mocked_guppy_data() for _ in "_"
        ).__anext__()
    )
    mocked_guppy_client.execute = mocked_execute_function
    app.state.guppy_client = mocked_guppy_client
