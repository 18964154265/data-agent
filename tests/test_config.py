import pytest

from data_agent_baseline.config import load_app_config, resolve_api_key


def test_environment_key_and_paths(tmp_path, monkeypatch):
    config = tmp_path / "settings.yaml"
    config.write_text(
        "agent:\n  api_key_env: TEST_MODEL_KEY\ndataset:\n  root_path: public/input\n"
    )
    monkeypatch.setenv("TEST_MODEL_KEY", "test-secret")
    settings = load_app_config(config)
    assert resolve_api_key(settings.agent) == "test-secret"
    assert settings.dataset.root_path.is_absolute()


@pytest.mark.parametrize(
    "payload",
    [
        "run:\n  max_workers: 0",
        "agent:\n  request_retries: -1",
        'etl:\n  use_cache: "false"',
        "agent:\n  typo: 1",
        "dataset:\n  task_ids: task_1",
    ],
)
def test_invalid_config_rejected(tmp_path, payload):
    path = tmp_path / "settings.yaml"
    path.write_text(payload)
    with pytest.raises(ValueError):
        load_app_config(path)
