# F02 · Weather API Client

Entry: `src/weather_client.py:99` — `get_weighted_weather(start_date, end_date)`

```mermaid
flowchart TD
    A["get_weighted_weather<br/>weather_client.py:99"]
    B["Initialize all_dfs list<br/>weather_client.py:104"]
    C["Iterate CITIES dict (4 cities)<br/>weather_client.py:106"]
    D["get_weather_for_city<br/>weather_client.py:107"]
    E["Extract city config<br/>weather_client.py:61"]
    F["client.weather_api<br/>weather_client.py:63"]
    G["HTTP POST archive-api.open-meteo.com<br/>weather_client.py:64"]
    H["Parse response.Hourly<br/>weather_client.py:75"]
    I["Build DatetimeIndex<br/>weather_client.py:78-82"]
    J["Create DataFrame from hourly vars<br/>weather_client.py:88-95"]
    K["Return city DataFrame<br/>weather_client.py:97"]
    L["Append to all_dfs<br/>weather_client.py:108"]
    M["pd.concat all DataFrames<br/>weather_client.py:110"]
    N["Group combined by datetime<br/>weather_client.py:114"]
    O["For each datetime group<br/>weather_client.py:114"]
    P["Iterate variable/weight pairs<br/>weather_client.py:117"]
    Q["Get city weights<br/>weather_client.py:123"]
    R["np.average weighted<br/>weather_client.py:125"]
    S["Store weighted_var<br/>weather_client.py:126"]
    T["Iterate CITIES for raw values<br/>weather_client.py:129"]
    U{"city_row empty?<br/>weather_client.py:131"}
    V["Extract temp/wind/radiation<br/>weather_client.py:132-134"]
    W["Append row to records<br/>weather_client.py:136"]
    X["Return records list<br/>weather_client.py:138"]

    A --> B --> C --> D --> E --> F --> G --> H --> I --> J --> K --> L --> C
    C -->|all cities done| M --> N --> O --> P --> Q --> R --> S --> P
    P -->|all vars done| T --> U
    U -->|empty| T
    U -->|not empty| V --> T
    T -->|all cities done| W --> O
    O -->|all groups done| X
```

## External Dependencies
- `openmeteo_requests.Client` — weather API client (line 54)
- `requests_cache.CachedSession` — HTTP cache 3600s TTL (line 52)
- `retry_requests.retry` — 5 retries, 0.2 backoff (line 53)
- `pandas` — DataFrame ops
- `numpy` — `np.average()` weighted calc (line 125)

## Side Effects
- HTTP response cached via `requests_cache` session
- No writes to external storage
