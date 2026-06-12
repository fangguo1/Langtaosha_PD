from __future__ import annotations

import importlib


def test_target_app_packages_are_importable():
    for module_name in (
        "app.routes",
        "app.pages",
        "app.dev",
    ):
        assert importlib.import_module(module_name).__name__ == module_name


def test_target_page_modules_are_importable_after_refactor():
    for module_name in (
        "app.pages.span_matcher_page",
        "app.pages.expanded_compare_page",
        "app.pages.feedback_review_page",
        "app.pages.retrieval_compare_page",
    ):
        assert importlib.import_module(module_name).__name__ == module_name


def test_target_route_modules_are_importable_after_refactor():
    for module_name in (
        "app.routes.paper",
        "app.routes.scholar",
        "app.routes.study",
    ):
        assert importlib.import_module(module_name).__name__ == module_name


def test_target_dev_modules_are_importable_after_refactor():
    for module_name in (
        "app.dev.develop_api_proxy",
        "app.dev.feedback_review_app",
        "app.dev.main_develop",
    ):
        assert importlib.import_module(module_name).__name__ == module_name
