from __future__ import annotations

from flask import Flask

from app.pages.expanded_compare_page import register_expanded_compare_page_routes


def test_expanded_compare_page_renders():
    app = Flask(__name__, template_folder="../../templates")
    register_expanded_compare_page_routes(app)

    response = app.test_client().get("/expanded-compare?q=renal%20adhesion")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Expanded Sparse Compare" in html
    assert "renal adhesion" in html
    assert "loose cov" in html


def test_expanded_compare_module_no_longer_exports_api_registrar():
    import app.pages.expanded_compare_page as page_module

    assert not hasattr(page_module, "register_expanded_compare_api_routes")
