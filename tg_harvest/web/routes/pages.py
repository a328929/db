from flask import redirect, render_template, request

from tg_harvest.web.auth import (
    admin_page_login_required,
    is_authenticated,
    normalize_admin_next_path,
)


def register_page_routes(app, *, page_size: int) -> None:
    @app.get("/")
    def index():
        return render_template("index.html", page_size=page_size)

    @app.get("/admin/login")
    def admin_login_page():
        next_path = normalize_admin_next_path(request.args.get("next"))
        if is_authenticated():
            return redirect(next_path)
        return render_template("admin_login.html", next_path=next_path)

    @app.get("/admin/manage")
    @admin_page_login_required
    def admin_manage_page():
        return render_template("admin_manage.html")

    @app.get("/admin/sync")
    @admin_page_login_required
    def admin_sync_page():
        return render_template("admin_sync.html")
