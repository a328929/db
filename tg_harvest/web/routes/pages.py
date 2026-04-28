# -*- coding: utf-8 -*-
from flask import render_template


def register_page_routes(app, *, page_size: int) -> None:
    @app.get("/")
    def index():
        return render_template("index.html", page_size=page_size)

    @app.get("/admin/manage")
    def admin_manage_page():
        return render_template("admin_manage.html")
