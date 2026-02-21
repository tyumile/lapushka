# Virtual Engineer v0.2

Новая версия в отдельном проекте, не затрагивает `virtual_engineer` (v0.1).

## Быстрый запуск

1. Скопируйте `.env.example` в `.env` и используйте те же ключи, что в v0.1.
2. Установите зависимости.
3. Запустите сервер:

```bash
python manage.py runserver
```

## Шаги UI

1. `Начало`
2. `Документы качества`
3. `Выбрать тип документов`
4. `План документов и содержимое` (P4B)
5. `Формирование документов`

## P4B Build Doc Plan

Порядок использования:

1. Загрузите проект и документы качества.
2. Запустите Process 2 и получите реестр качества.
3. На шаге выбора типов отметьте типы документов.
4. Запустите `Run P4B (Build Plan)`.
5. Отредактируйте инстансы документов/поля и нажмите `Save corrections`.
6. Проверьте `p4b_doc_instances_final.json`, затем переходите к генерации документов.

## Ключевые артефакты

- `02_processing/p2_quality_registry_v1.json`
- `02_processing/p2_quality_registry_final.json`
- `02_processing/p4_doc_types_selection.json`
- `02_processing/p4b_doc_instances_v1.json`
- `02_processing/p4b_doc_instances_final.json`
- `02_processing/p5_fill_plan.json`
- `04_logs/runs/<process>/<run_id>/...`
- `04_logs/edit_logs/edit_log_doc_plan.json`
- `03_output/<razdel_code>/...`
