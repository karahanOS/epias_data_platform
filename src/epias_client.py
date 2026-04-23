import os
import json                          # HATA DÜZELTİLDİ: eksik import eklendi
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import time
 
load_dotenv()
 
logger = logging.getLogger(__name__)
 
AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"
BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"
TGT_LIFETIME = timedelta(hours=1, minutes=30)
 
 
class EPIASClient:
    def __init__(self):
        self.username = os.getenv("EPIAS_USERNAME")
        self.password = os.getenv("EPIAS_PASSWORD")
        self.base_url = BASE_URL
        self.auth_url = AUTH_URL
        self._tgt = None
        self._token_time = None
 
    def _fetch_tgt(self):
        logger.info("EPIAŞ Auth: Yeni TGT alınıyor...")
        response = requests.post(
            self.auth_url,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain"},
            data={"username": self.username, "password": self.password}
        )
        response.raise_for_status()
        return response.text
 
    def _get_valid_tgt(self):
        now = datetime.now()
        token_expired = (
            self._tgt is None
            or self._token_time is None
            or (now - self._token_time) > TGT_LIFETIME
        )
        if token_expired:
            self._tgt = self._fetch_tgt()
            self._token_time = now
            logger.info("TGT başarıyla yenilendi.")
        return self._tgt
 
    def _post(self, endpoint, payload):
        headers = {
            "Authorization": f"Bearer {self._get_valid_tgt()}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 401:
                logger.warning("Token beklenmedik şekilde düştü, zorla yenileniyor...")
                self._tgt = self._fetch_tgt()
                self._token_time = datetime.now()
                return self._post(endpoint, payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API Hatası ({endpoint}): {str(e)}")
            return {"items": []}
 
    def format_date(self, date_str):
        """YYYY-MM-DD -> ISO 8601 (+03:00)"""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%S+03:00")
 
    # ── ENDPOINTler ───────────────────────────────────────────────────────────
 
    # HATA DÜZELTİLDİ: DAG'da "get_ptf_smf_sdf" olarak çağrılıyor,
    # metod adı ona göre eklendi (get_pricing_data alias'ı da korundu) 
    def get_pricing_data(self, start_date, end_date):
        payload = {"startDate": self.format_date(start_date), "endDate": self.format_date(end_date)}
        return self._post("/v1/data/ptf-smf-sdf", payload).get("items", [])
 
    def get_unlicensed_generation(self, start_date, end_date):
        payload = {"startDate": self.format_date(start_date), "endDate": self.format_date(end_date)}
        return self._post("/v1/renewables/data/unlicensed-generation-amount", payload).get("items", [])
 
    def get_imbalance_quantity(self, start_date, end_date):
        payload = {"startDate": self.format_date(start_date), "endDate": self.format_date(end_date)}
        return self._post("/v1/markets/imbalance/data/imbalance-quantity", payload).get("items", [])
 
    def get_dpp(self, start_date, end_date, org_id=None):
        payload = {"startDate": self.format_date(start_date), "endDate": self.format_date(end_date)}
        if org_id:
            payload["organizationId"] = org_id
        return self._post("/v1/generation/data/dpp", payload).get("items", [])
 
    def get_market_participants(self):
        return self._post("/v1/markets/general-data/data/market-participants", {}).get("items", [])
 
    def get_system_direction(self, start_date, end_date):
        payload = {"startDate": self.format_date(start_date), "endDate": self.format_date(end_date)}
        return self._post("/v1/markets/bpm/data/system-direction", payload).get("items", [])
 
    def get_realtime_generation(self, start_date: str, end_date: str) -> list:
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/generation/data/realtime-generation", body)
        data = result.get("items", [])
        numeric_cols = [
            "total", "naturalGas", "dammedHydro", "lignite", "river",
            "importCoal", "wind", "sun", "fueloil", "geothermal",
            "asphaltiteCoal", "blackCoal", "biomass", "naphta", "lng",
            "importExport", "wasteheat"
        ]
        for row in data:
            for col in numeric_cols:
                if col in row and row[col] is not None:
                    row[col] = float(row[col])
        return data
 
    def get_realtime_consumption(self, start_date: str, end_date: str) -> list:
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/consumption/data/realtime-consumption", body)
        return result.get("items", [])
 
    def get_load_estimation_plan(self, start_date: str, end_date: str) -> list:
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/consumption/data/load-estimation-plan", body)
        data = result.get("items", [])
        for row in data:
            if "lep" in row and row["lep"] is not None:
                row["lep"] = float(row["lep"])
        return data
 
    def get_uevcb_list(self):
        return self._post("/v1/generation/data/uevcb-list", {}).get("items", [])
 
    def get_organization_list(self):
        return self._post("/v1/generation/data/organization-list", {}).get("items", [])
 
 
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = EPIASClient()
    data = client.get_pricing_data("2024-01-01", "2024-01-01")
    for row in data:
        print(row)