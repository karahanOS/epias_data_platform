# F09 · Data Quality Monitor

Entry: `src/data_quality_check.py:302` — `main()`

```mermaid
flowchart TD
    Start["main()<br/>data_quality_check.py:302"]
    ParseArgs["parse --quick, --source, --gaps<br/>data_quality_check.py:303-307"]
    InitClient["get_client()<br/>data_quality_check.py:309"]
    PrintHeader["Print report header<br/>data_quality_check.py:311"]
    DetermineScope["Determine tables_to_check<br/>all HOURLY_TABLES or args.source<br/>data_quality_check.py:323-327"]
    ForEachTable["For each table<br/>data_quality_check.py:330"]
    CheckHourly["check_hourly_table(table)<br/>data_quality_check.py:100-146"]
    BuildSQL["Build SQL: COUNT, COUNTIF NULL,<br/>date range, completeness%<br/>data_quality_check.py:101-114"]
    RunQuery["client.query()<br/>data_quality_check.py:115"]
    QueryError{"Query error?<br/>data_quality_check.py:117"}
    CalcStats["completeness_pct, null_pct,<br/>freshness_days<br/>data_quality_check.py:121-125"]
    DetermineStatus["status: OK/WARN/ERROR<br/>data_quality_check.py:127-131"]
    PrintResult["print_hourly_result<br/>data_quality_check.py:332"]
    CheckGaps{"args.gaps?<br/>data_quality_check.py:335"}
    FindGaps["find_gaps(table, top_n=10)<br/>data_quality_check.py:149-163"]
    QuickMode{"args.quick?<br/>data_quality_check.py:343"}
    MartSection["Section 2: Gold Mart Tables<br/>data_quality_check.py:343"]
    CheckMart["check_mart(table)<br/>data_quality_check.py:188-227"]
    EventSection["Section 2b: Event Tables<br/>data_quality_check.py:368"]
    PTFSanity["ptf_sanity()<br/>data_quality_check.py:230-246"]
    WeatherCheck["check_weather()<br/>data_quality_check.py:249-266"]
    ScoreCard["Section 5: Özet Skor Kartı<br/>Count OK/WARN/ERROR<br/>data_quality_check.py:418-435"]
    Done["End<br/>data_quality_check.py:442"]

    Start --> ParseArgs --> InitClient --> PrintHeader --> DetermineScope
    DetermineScope --> ForEachTable --> CheckHourly
    CheckHourly --> BuildSQL --> RunQuery --> QueryError
    QueryError -->|Error| PrintResult
    QueryError -->|OK| CalcStats --> DetermineStatus --> PrintResult
    PrintResult --> CheckGaps
    CheckGaps -->|Yes| FindGaps --> ForEachTable
    CheckGaps -->|No| ForEachTable
    ForEachTable -->|all done| QuickMode
    QuickMode -->|No| MartSection --> CheckMart --> EventSection --> PTFSanity --> WeatherCheck --> ScoreCard --> Done
    QuickMode -->|Yes| PTFSanity
```

## Check Types
| Check | Target | Key Metric |
|---|---|---|
| `check_hourly_table` | 11 hourly staging tables | completeness% vs EXPECTED_HOURS (2025-01-01→today) |
| `find_gaps` | same | days with <24 hours |
| `check_mart` | 11 gold mart tables | freshness_days, row count |
| `ptf_sanity` | `stg_pricing` | avg/min/max/stddev PTF, outlier count |
| `check_weather` | `stg_weather` | per-city coverage |

## External Dependencies
- `google.cloud.bigquery` — BigQuery client
- `pandas`, `datetime`, `argparse`, `logging`
- Env: `GCP_PROJECT_ID`, `BQ_GOLD_DATASET`
