from django.urls import path

from .views_doc_types import doc_types_view
from .views_doc_plan import doc_plan_view, run_p4b_build_doc_plan_view, save_doc_plan_view
from .views_formation import formation_view
from .views_quality import quality_view
from .views_start import open_last_logs_view, start_view
from .views_utils import download_input_file_view, download_output_zip_view, project_status_view

urlpatterns = [
    path("start/", start_view, name="v02_start"),
    path("project/<str:project_id>/quality/", quality_view, name="v02_quality"),
    path("project/<str:project_id>/doc-types/", doc_types_view, name="v02_doc_types"),
    path("project/<str:project_id>/doc-plan/", doc_plan_view, name="v02_doc_plan"),
    path("project/<str:project_id>/run_p4b_build_doc_plan/", run_p4b_build_doc_plan_view, name="v02_run_p4b"),
    path("project/<str:project_id>/doc-plan/save/", save_doc_plan_view, name="v02_doc_plan_save"),
    path("project/<str:project_id>/formation/", formation_view, name="v02_formation"),
    path("project/<str:project_id>/download-output/", download_output_zip_view, name="v02_download_output"),
    path("project/<str:project_id>/input/<path:relative_path>", download_input_file_view, name="v02_input_file"),
    path("project/<str:project_id>/status/", project_status_view, name="v02_project_status"),
    path("project/<str:project_id>/logs-last/", open_last_logs_view, name="v02_logs_last"),
]
