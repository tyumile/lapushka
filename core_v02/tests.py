from django.test import Client, TestCase, override_settings

from .services.io_utils import read_processing
from .services.process_p2_quality_registry import _normalize_payload
from .services.process_p2_quality_registry import run_process_p2
from .services.process_p4b_build_doc_plan import run_process_p4b_build_doc_plan
from .services.process_p5_fill_plan import run_process_p5
from .services.project_storage import create_project_structure, load_project_meta, save_processing_json, save_project_meta
from .views_utils import resolve_files_for_process
from pathlib import Path


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


class V02P2NormalizationTests(TestCase):
    def test_protocol_name_is_remapped_to_real_quality_filename(self):
        quality_files = [
            Path("Протокол испытаний №1 от 30.09.2025 г. pdf"),
            Path("Протокол испытаний №2 от 12.11.2025 г. pdf"),
        ]
        payload = {
            "project_cipher": {"value": "жилой комплекс по адресу", "status": "ok", "confidence": 1.0, "source": {}},
            "razdel": {"razdel_code": "KJ", "razdel_name": "КЖ", "status": "ok", "confidence": 1, "source": {}},
            "materials": [
                {
                    "material_id": "mat-001",
                    "material_name": "Бетон B25",
                    "docs": [
                        {
                            "doc_kind": "Протокол",
                            "doc_number": "1",
                            "doc_date": "30.09.2025",
                            "volume": "12 суток",
                            "manufacturer": "x",
                            "issuer": "y",
                            "file_ref": "01_input/02_quality_docs/protocol1.pdf",
                            "status": "ok",
                            "confidence": 1.0,
                            "source": {"file": "protocol1.pdf", "page": "1", "snippet": ""},
                        }
                    ],
                }
            ],
        }
        out = _normalize_payload(payload, quality_files, "Project__123")
        refs = {d.get("file_ref") for m in out.get("materials") or [] for d in m.get("docs") or []}
        self.assertIn("01_input/02_quality_docs/Протокол испытаний №1 от 30.09.2025 г. pdf", refs)
        self.assertIn("01_input/02_quality_docs/Протокол испытаний №2 от 12.11.2025 г. pdf", refs)
        self.assertEqual((out.get("project_cipher") or {}).get("status"), "needs_extraction")
