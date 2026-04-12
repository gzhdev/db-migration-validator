import os

from flask import Flask, render_template, request, redirect, url_for, jsonify, abort, send_file

import db
import importer

app = Flask(__name__)


@app.before_request
def ensure_db():
    db.init_db()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    runs = db.list_runs()
    return render_template("index.html", runs=runs)


@app.route("/import", methods=["POST"])
def import_run():
    debug_dir = request.form.get("debug_dir", "").strip()
    if not debug_dir:
        return redirect(url_for("index"))
    try:
        run_id = importer.import_debug_dir(debug_dir)
    except ValueError as e:
        runs = db.list_runs()
        return render_template("index.html", runs=runs, error=str(e))
    return redirect(url_for("run_detail", run_id=run_id))


@app.route("/runs/<int:run_id>")
def run_detail(run_id):
    run = db.get_run(run_id)
    if not run:
        abort(404)
    summaries = db.get_table_summaries(run_id)
    return render_template("run_detail.html", run=run, summaries=summaries)


@app.route("/runs/<int:run_id>/records")
def records_page(run_id):
    run = db.get_run(run_id)
    if not run:
        abort(404)
    table_names = db.get_table_names(run_id)
    selected_table = request.args.get("table", "")
    selected_type = request.args.get("match_type", "")
    return render_template(
        "records.html",
        run=run,
        table_names=table_names,
        selected_table=selected_table,
        selected_type=selected_type,
    )


@app.route("/mapping-generator")
def mapping_generator():
    return render_template("mapping_generator.html")


@app.route("/api/csv-template")
def csv_template():
    csv_path = os.path.join(
        os.path.dirname(__file__), os.pardir,
        ".claude", "skills", "resources", "gen-mapping", "csv_template.csv"
    )
    csv_path = os.path.normpath(csv_path)
    if os.path.isfile(csv_path):
        return send_file(csv_path, mimetype="text/csv", as_attachment=True,
                         download_name="csv_template.csv")
    # Fallback: inline minimal template
    header = ("source_table,target_table,source_field,target_field,is_primary_key,"
              "transform_type,transform_fields,transform_params,compare_rule,"
              "tolerance,nullable,description\n")
    return header, 200, {"Content-Type": "text/csv",
                         "Content-Disposition": "attachment; filename=csv_template.csv"}


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------

@app.route("/api/runs/<int:run_id>/records")
def api_records(run_id):
    run = db.get_run(run_id)
    if not run:
        abort(404)
    table = request.args.get("table", "")
    match_type = request.args.get("match_type", "")
    pk_search = request.args.get("pk_search", "").strip()
    try:
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 20))
    except ValueError:
        page, page_size = 1, 20

    result = db.query_records(
        run_id,
        table_name=table or None,
        match_type=match_type or None,
        pk_search=pk_search or None,
        page=page,
        page_size=page_size,
    )
    return jsonify(result)


@app.route("/runs/<int:run_id>", methods=["DELETE"])
def delete_run(run_id):
    run = db.get_run(run_id)
    if not run:
        abort(404)
    db.delete_run(run_id)
    return jsonify({"ok": True})


if __name__ == "__main__":
    db.init_db()
    app.run(debug=True, port=5000)
