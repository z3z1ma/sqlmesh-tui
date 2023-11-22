import pytest
from sqlmesh import Context

from sqlmesh_tui.app import SQLMeshApp


@pytest.fixture
def ctx():
    return Context(paths=["tests/fixtures/project"])


@pytest.mark.asyncio
async def test_keys(ctx):
    """Test pressing keys has the desired result."""

    app = SQLMeshApp(ctx)
    async with app.run_test() as pilot:
        # Ensure initial state is correct
        assert app.dark is True

        # Press "d" to toggle dark mode
        await pilot.press("d")
        assert app.dark is False
