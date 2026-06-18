"""
fx_client.py — TCMB XML USD/TRY Kur Istemcisi
================================================
TCMB'nin gunluk kur XML dosyalarindan resmi USD alis kurunu ceker.
Endpoint: https://www.tcmb.gov.tr/kurlar/YYYYMM/DDMMYYYY.xml

API key gerektirmez. Resmi TCMB verileri.
Hafta sonu / resmi tatil: HTTP 404 doner, bir onceki is gunune gidilir.
"""

import logging
import requests
from datetime import datetime, timedelta
from xml.etree import ElementTree

logger = logging.getLogger(__name__)

_TCMB_BASE = "https://www.tcmb.gov.tr/kurlar"


class FXClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "epias-data-platform/1.0"})

    def get_usdtry(self, date: str) -> list[dict]:
        """
        Verilen tarih icin USD/TRY alis kurunu doner (YYYY-MM-DD).
        Tatil/hafta sonu -> geri dogu en fazla 5 is gunu arar.
        Sonuc her zaman tek elemanli liste [{date, usdtry, is_ffilled}] doner.
        """
        for delta in range(6):
            target = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=delta)
            result = self._fetch(target)
            if result is not None:
                return [{
                    "date":       date,
                    "usdtry":     result,
                    "is_ffilled": delta > 0,
                    "source":     "TCMB_XML",
                }]

        logger.error("TCMB XML: %s icin 5 gun geriye gidildi, veri bulunamadi.", date)
        return []

    def get_usdtry_range(self, start: str, end: str) -> list[dict]:
        """
        Tarih araligi icin tum is-gunu kurlarini ceker.
        Hafta sonlari otomatik atlanir.
        """
        import time
        rows = []
        cur = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")

        while cur <= end_dt:
            # Hafta sonu atla (Pazartesi=0 ... Pazar=6)
            if cur.weekday() < 5:
                result = self._fetch(cur)
                if result is not None:
                    rows.append({
                        "date":       cur.strftime("%Y-%m-%d"),
                        "usdtry":     result,
                        "is_ffilled": False,
                        "source":     "TCMB_XML",
                    })
                time.sleep(0.15)
            cur += timedelta(days=1)

        return rows

    def _fetch(self, dt: datetime) -> float | None:
        """
        Tek gun icin TCMB XML dosyasini ceker.
        404 -> tatil/hafta sonu, None doner.
        """
        url = f"{_TCMB_BASE}/{dt.strftime('%Y%m')}/{dt.strftime('%d%m%Y')}.xml"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            root = ElementTree.fromstring(resp.content)
            for currency in root.findall(".//Currency[@Kod='USD']"):
                node = currency.find("ForexBuying")
                if node is not None and node.text and node.text.strip():
                    return float(node.text.replace(",", "."))
        except requests.RequestException as exc:
            logger.warning("TCMB XML hata (%s): %s", dt.strftime("%Y-%m-%d"), exc)
        return None


if __name__ == "__main__":
    import json, logging
    logging.basicConfig(level=logging.INFO)
    client = FXClient()
    print(json.dumps(client.get_usdtry("2025-01-15"), indent=2))
