from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException

from pipeline.step1_config import load_step1_config, save_config

from mpstats_app.api.dependencies import get_classifier_rules_service, get_manual_overrides_service, get_settings
from mpstats_app.config import AppSettings
from mpstats_app.schemas import ClassifierRulesPayload, ManualOverridesPayload, TextPayload
from mpstats_app.services.classifier_rules_service import ClassifierRulesService
from mpstats_app.services.manual_overrides_service import ManualOverridesService


router = APIRouter(prefix="/api", tags=["settings"])


@router.get("/settings/export-config")
def get_export_config(settings: AppSettings = Depends(get_settings)) -> dict[str, object]:
    try:
        config = load_step1_config(settings.config_path)
    except FileNotFoundError:
        config = {}
    return {"path": str(settings.config_path), "config": config}


@router.put("/settings/export-config")
def put_export_config(payload: dict[str, object], settings: AppSettings = Depends(get_settings)) -> dict[str, object]:
    try:
        path = save_config(payload, settings.config_path)
        return {"path": str(path), "config": load_step1_config(path)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/rules")
def get_rules(settings: AppSettings = Depends(get_settings)) -> dict[str, object]:
    content = settings.rules_path.read_text(encoding="utf-8") if settings.rules_path.exists() else ""
    return {"path": str(settings.rules_path), "content": content}


@router.put("/rules")
def put_rules(payload: TextPayload, settings: AppSettings = Depends(get_settings)) -> dict[str, object]:
    try:
        settings.rules_path.parent.mkdir(parents=True, exist_ok=True)
        settings.rules_path.write_text(payload.content, encoding="utf-8")
        return {"path": str(settings.rules_path), "content": payload.content}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=json.dumps({"error": str(exc)}, ensure_ascii=False)) from exc


@router.get("/classifier/rules")
def get_classifier_rules(
    service: ClassifierRulesService = Depends(get_classifier_rules_service),
) -> dict[str, object]:
    return service.list_rules()


@router.put("/classifier/rules")
def put_classifier_rules(
    payload: ClassifierRulesPayload,
    service: ClassifierRulesService = Depends(get_classifier_rules_service),
) -> dict[str, object]:
    try:
        return service.save_rules([rule.model_dump() for rule in payload.rules])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/classifier/manual-overrides")
def get_manual_overrides(
    service: ManualOverridesService = Depends(get_manual_overrides_service),
) -> dict[str, object]:
    return service.list_overrides()


@router.put("/classifier/manual-overrides")
def put_manual_overrides(
    payload: ManualOverridesPayload,
    service: ManualOverridesService = Depends(get_manual_overrides_service),
) -> dict[str, object]:
    try:
        return service.save_overrides([override.model_dump() for override in payload.overrides])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
