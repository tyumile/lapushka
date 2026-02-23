from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase, override_settings

from .services.io_utils import read_processing
from .services.process_p2_quality_registry import run_process_p2
from .services.process_p4b_build_doc_plan import (
    _build_request_inputs,
    run_process_p4b_build_doc_plan,
    validate_p4_plan_payload,
)
from .services.process_p5_fill_plan import run_process_p5
from .services.project_storage import (
    create_project_structure,
    list_uploaded_files,
    load_project_meta,
    project_root,
    save_processing_json,
    save_project_meta,
)
from .views_utils import resolve_files_for_process


@override_settings(MOCK_MODE=True)
class V02ContractsTest(TestCase):
    def setUp(self):
        self.project_id = create_project_structure("Test v02")
        meta = load_project_meta(self.project_id)
        meta["comment"] = "test"
        save_project_meta(self.project_id, meta)

    def test_process2_contract(self):
        _, p2 = run_process_p2(self.project_id, "test")
        self.assertIn("project_cipher", p2)
        self.assertIn("razdel", p2)
        self.assertIn("materials", p2)
        self.assertIsInstance(p2["materials"], list)

    def test_process4b_contract(self):
        _, p2 = run_process_p2(self.project_id, "test")
        _, p4, _ = run_process_p4b_build_doc_plan(
            self.project_id,
            (p2.get("razdel") or {}).get("razdel_code", "KJ"),
            ["acts_hidden"],
        )
        self.assertIn("doc_instances", p4)
        self.assertIn("selected_doc_type_ids", p4)

    def test_process5_contract(self):
        _, p2 = run_process_p2(self.project_id, "test")
        _, p4, _ = run_process_p4b_build_doc_plan(
            self.project_id,
            (p2.get("razdel") or {}).get("razdel_code", "KJ"),
            ["acts_hidden"],
        )
        _, p5 = run_process_p5(
            self.project_id,
            (p2.get("razdel") or {}).get("razdel_code", "KJ"),
            p4,
            p2,
            resolve_files_for_process(self.project_id),
        )
        self.assertIn("outputs", p5)
        self.assertIsInstance(p5["outputs"], list)


@override_settings(MOCK_MODE=True)
class V02E2ESmokeTest(TestCase):
    def test_e2e_flow(self):
        c = Client()
        resp = c.post("/start/", {"action": "create_project", "project_name": "Smoke", "comment": "Run"})
        self.assertEqual(resp.status_code, 302)
        location = resp.headers.get("Location", "")
        self.assertIn("/quality/", location)
        project_id = location.strip("/").split("/")[-2]

        resp = c.post(f"/project/{project_id}/quality/", {"action": "run_p2"})
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/quality/", resp.headers.get("Location", ""))

        resp = c.post(f"/project/{project_id}/quality/", {"action": "next_to_doc_types"})
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/doc-types/", resp.headers.get("Location", ""))

        resp = c.post(f"/project/{project_id}/doc-types/", {"action": "run_p4b", "doc_type_ids": ["acts_hidden"]})
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/doc-plan/", resp.headers.get("Location", ""))

        p4 = read_processing(project_id, "p4b_doc_instances_final.json", {})
        docs_count = len(p4.get("doc_instances") or [])
        row_order = ",".join(str(i) for i in range(docs_count))
        payload = {"action": "save_doc_list_edits", "rows_order": row_order}
        for i, row in enumerate(p4.get("doc_instances") or []):
            payload[f"doc_name_{i}"] = row.get("doc_name", "")
            payload[f"doc_number_suggestion_{i}"] = row.get("doc_number", "")
            payload[f"work_scope_{i}"] = "[]"
            payload[f"fields_{i}"] = "{}"
        resp = c.post(f"/project/{project_id}/doc-plan/save/", payload)
        self.assertEqual(resp.status_code, 302)

        resp = c.post(f"/project/{project_id}/formation/", {"action": "run_p5"})
        self.assertEqual(resp.status_code, 302)
        fill_plan = read_processing(project_id, "p5_fill_plan.json", {})
        self.assertIn("outputs", fill_plan)


@override_settings(MOCK_MODE=True)
class V02P4BTests(TestCase):
    def setUp(self):
        self.project_id = create_project_structure("P4B test")
        meta = load_project_meta(self.project_id)
        meta["comment"] = "comment"
        save_project_meta(self.project_id, meta)

    def test_view_save_doc_plan_creates_final(self):
        save_processing_json(
            self.project_id,
            "p4b_doc_instances_final.json",
            {
                "razdel_code": "KJ",
                "selected_doc_type_ids": ["acts_hidden"],
                "doc_instances": [
                    {
                        "instance_id": "i1",
                        "doc_id": "AOSR",
                        "doc_type_id": "acts_hidden",
                        "doc_name": "name",
                        "doc_number": "",
                        "multiplier": {"axis": "section", "label": "S1", "confidence": 0.5},
                        "work_scope": [],
                        "fields": {},
                        "overall_status": "needs_extraction",
                        "evidence": [],
                    }
                ],
                "open_questions": [],
                "issues": [],
            },
        )
        c = Client()
        resp = c.get(f"/project/{self.project_id}/doc-plan/")
        self.assertEqual(resp.status_code, 200)
        resp = c.post(
            f"/project/{self.project_id}/doc-plan/save/",
            {
                "rows_order": "0",
                "doc_name_0": "edited",
                "doc_number_0": "1",
                "mult_axis_0": "section",
                "mult_label_0": "S1",
                "work_scope_0": "[]",
                "fields_0": "{}",
            },
        )
        self.assertEqual(resp.status_code, 302)
        saved = read_processing(self.project_id, "p4b_doc_instances_final.json", {})
        self.assertEqual((saved.get("doc_instances") or [])[0].get("doc_name"), "edited")


@override_settings(MOCK_MODE=True)
class V02P4ValidationTests(TestCase):
    def setUp(self):
        self.project_id = create_project_structure("P4 validation")

    def test_validate_p4_payload_valid_and_invalid(self):
        valid_payload = {
            "process": "p4_plan_docs",
            "razdel_code": "KJ",
            "work_breakdown": [
                {
                    "work_group_id": "kj_foundation",
                    "work_type_id": "kj_rebar",
                    "work_name": "армирование",
                    "status": "needs_extraction",
                    "evidence": {"file": "", "page": None, "snippet": None},
                }
            ],
            "doc_instances": [
                {
                    "doc_id": "AOSR",
                    "doc_variant_id": None,
                    "doc_type_id": "acts_hidden",
                    "doc_type_name": "Акты",
                    "doc_name": "Акт",
                    "basis": "from_work_requirement",
                    "instance_key": "a1",
                    "multiplicity": "single",
                    "linked_work_type_ids": ["kj_rebar"],
                    "scope": {},
                    "fields": {
                        "f1": {
                            "value": None,
                            "style_source": "standard",
                            "evidence": {"file": "", "page": None, "snippet": None},
                        }
                    },
                    "status": "needs_extraction",
                    "evidence": {"file": "", "page": None, "snippet": None},
                }
            ],
            "questions": [],
        }
        is_valid, _ = validate_p4_plan_payload(valid_payload)
        self.assertTrue(is_valid)

        invalid_payload = dict(valid_payload)
        invalid_payload["doc_instances"] = [dict(valid_payload["doc_instances"][0], status="ok", evidence={"file": "", "page": 1, "snippet": "x"})]
        is_valid, reason = validate_p4_plan_payload(invalid_payload)
        self.assertFalse(is_valid)
        self.assertIn("status=ok", reason)

    def test_xlsx_is_not_uploaded_as_input_file(self):
        root = project_root(self.project_id)
        sample_xlsx = root / "01_input" / "04_samples" / "KJ" / "id" / "sample.xlsx"
        sample_xlsx.parent.mkdir(parents=True, exist_ok=True)
        sample_xlsx.write_bytes(b"fake")

        with patch("core_v02.services.process_p4b_build_doc_plan._xlsx_to_json_text", return_value='{"ok":true}') as mock_conv:
            upload_paths, input_texts, _ = _build_request_inputs(self.project_id, "KJ", [sample_xlsx])

        self.assertEqual(upload_paths, [])
        self.assertTrue(any("sample_xlsx_json::" in x for x in input_texts))
        self.assertTrue(mock_conv.called)


@override_settings(MOCK_MODE=True)
class V02QualityUploadTests(TestCase):
    def setUp(self):
        self.project_id = create_project_structure("Quality upload")

    def test_upload_quality_requires_selected_files(self):
        c = Client()
        resp = c.post(f"/project/{self.project_id}/quality/", {"action": "upload_quality"})
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/quality/", resp.headers.get("Location", ""))

    def test_upload_quality_with_named_field_saves_file(self):
        c = Client()
        uploaded = SimpleUploadedFile("q1.txt", b"quality")
        resp = c.post(
            f"/project/{self.project_id}/quality/",
            {"action": "upload_quality", "quality_files": [uploaded]},
        )
        self.assertEqual(resp.status_code, 302)
        files_map = list_uploaded_files(self.project_id)
        self.assertIn("01_input/02_quality_docs/q1.txt", files_map.get("quality") or [])
