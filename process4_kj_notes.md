# Разбор Process 4 и словаря КЖ

## 1) Где агент в Process 4 формирует полный список работ

В актуальном расширенном сценарии это происходит в `process_p4b` через промпт `04b_p4b_build_doc_instances`.

Ключевая точка в промпте:
- **Шаг 1**: «Построить разбиение работ КЖ (work_breakdown)» и «Собери список работ, по которым требуется ИД в КЖ».

## 2) Какой промпт отправляется агенту

### Для `process_4` (базовый)
Сервис вызывает промпт `03_p4_doc_list_v02` и передаёт переменные:
- `selected_doc_type_ids`
- `dictionary_json`
- `quality_registry_json`

Содержательно этот промпт просит собрать `p4_doc_list_v1` (список документов к генерации + комментарии агента с source).

### Для `process_p4b` (расширенный, с полным списком работ)
Сервис вызывает:
- system prompt: `01_system_v02`
- user prompt: `04b_p4b_build_doc_instances`

Переменные в user prompt:
- `project_id`
- `razdel_code`
- `razdel_name`
- `selected_doc_type_ids_json`
- `user_comment`
- `dictionary_hint`
- `quality_registry_hint`
- `regs_hint`
- `samples_hint`

Именно этот промпт требует построения `work_breakdown` и дальнейшего сопоставления работ с экземплярами документов.


## 2.1) Сам prompt (без пересказа)

Полный текст user-prompt, который используется в расширенном процессе со сборкой полного `work_breakdown`, находится в файле:

- `core_v02/llm/prompts/04b_p4b_build_doc_instances.txt`

Чтобы вывести его в терминал без сокращений:

```bash
cat core_v02/llm/prompts/04b_p4b_build_doc_instances.txt
```

## 3) Какие документы предполагаются в словаре по разделу КЖ

Раздел `KJ / КЖ` содержит следующие группы и документы (`doc_id`):

1. `acts_hidden`
   - `AOSR` — Акт освидетельствования скрытых работ

2. `acts_responsible_structures`
   - `AOOK` — Акт освидетельствования ответственных конструкций

3. `KJ_REGISTERS`
   - `KJ_REESTR_ID` — Реестр исполнительной документации
   - `KJ_REESTR_AOSR` — Реестр актов освидетельствования скрытых работ (АОСР)
   - `KJ_REESTR_CERTS` — Реестр сертификатов и паспортов качества
   - `KJ_REESTR_SCHEMES_TESTS` — Реестр исполнительных схем и протоколов испытаний

4. `KJ_TITLE_SHEETS`
   - `KJ_TITLE_SHEET` — Титульный лист тома исполнительной документации

5. `KJ_EXEC_SCHEMES`
   - `KJ_EXEC_SCHEME_GEODETIC` — Исполнительные геодезические схемы
   - `KJ_EXEC_SCHEME_REBAR` — Исполнительные схемы армирования

6. `KJ_PROTOCOLS_TESTS`
   - `KJ_PROTOCOL_BETON` — Протоколы лабораторных испытаний бетона
   - `KJ_TEMP_SHEETS` — Температурные листы бетонных работ

7. `KJ_INPUT_CONTROL`
   - `KJ_VIK_ACT` — Акт входного контроля материалов и конструкций
   - `KJ_VIK_JOURNAL` — Журнал входного контроля материалов и конструкций

8. `KJ_APPENDICES`
   - `KJ_REESTR_DOCS_QUALITY` — Реестр документов качества (приложение к АОСР)
