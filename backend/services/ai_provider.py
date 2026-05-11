from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from openai import AsyncAzureOpenAI, AsyncOpenAI, AzureOpenAI, OpenAI

AZURE_OPENAI_API_VERSION = "2024-02-15-preview"
DEFAULT_AZURE_OPENAI_DEPLOYMENT_NAME = "gpt-4o"
DEFAULT_LEGACY_AI_MODEL = "gpt-4o-mini"


@dataclass(frozen=True)
class AIProviderSettings:
    provider: str
    api_key: str
    endpoint: str = ""
    deployment_name: str = ""
    api_version: str = AZURE_OPENAI_API_VERSION


def _read_config(config_path: Optional[Path | str]) -> dict[str, Any]:
    if config_path is None:
        return {}
    try:
        config_file = config_path if isinstance(config_path, Path) else Path(config_path)
    except Exception:
        return {}
    try:
        with config_file.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def resolve_ai_provider_settings(
    config_path: Optional[Path | str] = None,
    model_name_override: Optional[str] = None,
) -> Optional[AIProviderSettings]:
    config = _read_config(config_path)

    azure_cfg = config.get("azure_openai", {}) if isinstance(config, dict) else {}
    openai_cfg = config.get("openai", {}) if isinstance(config, dict) else {}

    azure_api_key = str(os.environ.get("AZURE_OPENAI_API_KEY") or azure_cfg.get("api_key", "") or "").strip()
    azure_endpoint = str(os.environ.get("AZURE_OPENAI_ENDPOINT") or azure_cfg.get("endpoint", "") or "").strip()
    azure_deployment_name = str(
        os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME")
        or azure_cfg.get("deployment_name", "")
        or openai_cfg.get("model", "")
        or DEFAULT_AZURE_OPENAI_DEPLOYMENT_NAME
        or ""
    ).strip()
    azure_api_version = str(
        os.environ.get("AZURE_OPENAI_API_VERSION")
        or azure_cfg.get("api_version", "")
        or AZURE_OPENAI_API_VERSION
    ).strip() or AZURE_OPENAI_API_VERSION

    if azure_api_key and azure_endpoint:
        return AIProviderSettings(
            provider="azure",
            api_key=azure_api_key,
            endpoint=azure_endpoint,
            deployment_name=azure_deployment_name or DEFAULT_AZURE_OPENAI_DEPLOYMENT_NAME,
            api_version=azure_api_version,
        )

    legacy_api_key = str(os.environ.get("OPENAI_API_KEY") or openai_cfg.get("api_key", "") or "").strip()
    legacy_model = str(openai_cfg.get("model", "") or model_name_override or DEFAULT_LEGACY_AI_MODEL).strip() or DEFAULT_LEGACY_AI_MODEL
    if legacy_api_key:
        return AIProviderSettings(
            provider="openai",
            api_key=legacy_api_key,
            deployment_name=legacy_model,
        )

    return None


def create_sync_ai_client(
    config_path: Optional[Path | str] = None,
    model_name_override: Optional[str] = None,
) -> tuple[Optional[object], str, str]:
    settings = resolve_ai_provider_settings(config_path=config_path, model_name_override=model_name_override)
    if settings is None:
        return None, str(model_name_override or DEFAULT_LEGACY_AI_MODEL).strip() or DEFAULT_LEGACY_AI_MODEL, "none"

    if settings.provider == "azure":
        return (
            AzureOpenAI(
                api_key=settings.api_key,
                azure_endpoint=settings.endpoint,
                api_version=settings.api_version,
            ),
            settings.deployment_name,
            settings.provider,
        )

    return OpenAI(api_key=settings.api_key), settings.deployment_name or DEFAULT_LEGACY_AI_MODEL, settings.provider


def create_async_ai_client(
    config_path: Optional[Path | str] = None,
    model_name_override: Optional[str] = None,
) -> tuple[Optional[object], str, str]:
    settings = resolve_ai_provider_settings(config_path=config_path, model_name_override=model_name_override)
    if settings is None:
        return None, str(model_name_override or DEFAULT_LEGACY_AI_MODEL).strip() or DEFAULT_LEGACY_AI_MODEL, "none"

    if settings.provider == "azure":
        return (
            AsyncAzureOpenAI(
                api_key=settings.api_key,
                azure_endpoint=settings.endpoint,
                api_version=settings.api_version,
            ),
            settings.deployment_name,
            settings.provider,
        )

    return AsyncOpenAI(api_key=settings.api_key), settings.deployment_name or DEFAULT_LEGACY_AI_MODEL, settings.provider


def has_any_ai_credentials(config_path: Optional[Path | str] = None) -> bool:
    return resolve_ai_provider_settings(config_path=config_path) is not None


def has_azure_ai_credentials(config_path: Optional[Path | str] = None) -> bool:
    config = _read_config(config_path)
    azure_cfg = config.get("azure_openai", {}) if isinstance(config, dict) else {}
    azure_api_key = str(os.environ.get("AZURE_OPENAI_API_KEY") or azure_cfg.get("api_key", "") or "").strip()
    azure_endpoint = str(os.environ.get("AZURE_OPENAI_ENDPOINT") or azure_cfg.get("endpoint", "") or "").strip()
    return bool(azure_api_key and azure_endpoint)