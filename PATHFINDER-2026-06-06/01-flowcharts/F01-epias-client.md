# F01 · EPIAS API Client

Entry: `src/epias_client.py:24` — `EPIASClient`

## Happy Path: `get_ptf_smf_sdf(start_date, end_date)`

```mermaid
flowchart TD
    A["get_ptf_smf_sdf<br/>epias_client.py:203"]
    B["_date_body<br/>epias_client.py:190"]
    C["_to_iso (start)<br/>epias_client.py:181"]
    D["_to_iso (end)<br/>epias_client.py:181"]
    E["_post<br/>epias_client.py:84"]
    F["Retry Loop MAX_RETRIES=3<br/>epias_client.py:97"]
    G["_get_valid_tgt<br/>epias_client.py:69"]
    H{"Cache Valid?<br/>epias_client.py:72-75"}
    I["_fetch_tgt<br/>epias_client.py:46"]
    J["POST AUTH_URL<br/>epias_client.py:99"]
    K["POST BASE_URL with TGT<br/>epias_client.py:99"]
    L{"Status Check<br/>epias_client.py:109-114"}
    M["401 Retry<br/>epias_client.py:111"]
    N["r.json()<br/>epias_client.py:141"]
    O["Extract items<br/>epias_client.py:144-154"]
    P["Return items[]<br/>epias_client.py:212"]
    Q["400 Business Error<br/>epias_client.py:116-129"]
    R["Return Empty<br/>epias_client.py:129"]
    S["Timeout/ConnError/5xx<br/>epias_client.py:156-166"]
    V["Exponential Backoff<br/>epias_client.py:169-171"]
    W["Raise RuntimeError<br/>epias_client.py:173-176"]

    A --> B --> C & D --> E --> F --> G
    G --> H
    H -->|expired| I --> J --> I
    H -->|valid| K
    I --> K
    K --> L
    L -->|200-299| N --> O --> P
    L -->|401| M --> F
    L -->|400| Q --> R --> P
    L -->|5xx/timeout| S --> V --> F
    F -->|max retries| W
```

## External Dependencies
- `requests` — HTTP POST (lines 49, 99)
- `dotenv` — `.env` loading (line 11)
- `datetime`, `time`, `os`, `json`, `logging`, `typing`
- Constants: `AUTH_URL:16`, `BASE_URL:17`, `TGT_LIFETIME`, `MAX_RETRIES=3`, `RETRY_BACKOFF=2.0`

## Side Effects
- Token cache written: `self._tgt`, `self._token_time` (lines 77–78)
