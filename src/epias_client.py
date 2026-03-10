import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import time

load_dotenv()  # .env dosyasından USERNAME ve PASSWORD'ü yükle

logger = logging.getLogger(__name__)

AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"
BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"
TGT_LIFETIME = timedelta(hours=1, minutes=30)  # 2 saat yerine 1.5 — güvenli tampon


class EPIASClient:
    def __init__(self):
        self.username = os.getenv("EPIAS_USERNAME")
        self.password = os.getenv("EPIAS_PASSWORD")

        # Token bellekte tutulur — dotenv'e yazmıyoruz çünkü her oturumda değişiyor
        self._tgt = None
        self._token_time = None  # Token'ı ne zaman aldık?

    # ── AUTH ──────────────────────────────────────────────────────────────────

    def _fetch_tgt(self) -> str:
        """Auth sunucusundan yeni bir TGT token alır."""
        logger.info("Yeni TGT alınıyor...")
        response = requests.post(
            AUTH_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "text/plain",
            },
            data={
                "username": self.username,
                "password": self.password,
            },
        )
        response.raise_for_status()  # 4xx/5xx hatası varsa exception fırlat
        return response.text.strip()

    def _get_valid_tgt(self) -> str:
        """
        Proactive refresh: token yoksa veya 1.5 saat geçtiyse yenile,
        aksi halde mevcut token'ı döndür.
        
        Pasaport analojisi: seyahatten önce kontrol et, dolmuşsa önceden yenile.
        """
        now = datetime.now()
        token_expired = (
            self._tgt is None                              # Hiç token almadık
            or self._token_time is None                    # Zaman kaydı yok
            or (now - self._token_time) > TGT_LIFETIME    # 1.5 saat doldu
        )

        if token_expired:
            self._tgt = self._fetch_tgt()
            self._token_time = now
            logger.info("TGT yenilendi.")

        return self._tgt

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _post(self, endpoint: str, body: dict, retry: bool = True) -> dict:
        """
        POST isteği atar. 401 gelirse token'ı yenileyip bir kez daha dener.
        
        retry=True  → 401 gelince TGT yenile ve tekrar dene (retry on failure)
        retry=False → ikinci denemede yine 401 gelirse exception fırlat
        """
        tgt = self._get_valid_tgt()

        response = requests.post(
            f"{BASE_URL}{endpoint}",
            headers={"TGT": tgt, "Content-Type": "application/json"},
            json=body,
        )

        if response.status_code == 401 and retry:
            # Token beklenmedik şekilde expire olmuş — zorla yenile
            logger.warning("401 alındı, TGT yenileniyor ve tekrar deneniyor...")
            self._tgt = None  # Cache'i temizle, _get_valid_tgt yeniden alacak
            return self._post(endpoint, body, retry=False)  # Bir kez daha dene

        response.raise_for_status()
        return response.json()

    # ── ENDPOINTler ───────────────────────────────────────────────────────────

    def get_ptf(self, start_date: str, end_date: str) -> list:
        """
        PTF (Piyasa Takas Fiyatı) verisini çeker.
        
        Kullanım:
            client.get_ptf("2024-01-01T00:00:00+03:00", "2024-01-01T23:00:00+03:00")
        """
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/markets/day-ahead/data/mcp", body)
        return result.get("body", {}).get("mCPList", [])

    def get_smf(self, start_date: str, end_date: str) -> list:
        """SMF (Sistem Marjinal Fiyatı) verisini çeker."""
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/markets/bpm/data/system-marginal-price", body)
        return result.get("body", {}).get("systemMarginalPriceList", [])

    def get_realtime_generation(self, start_date: str, end_date: str) -> list:
        """Gerçek zamanlı üretim verisini kaynak bazında çeker."""
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/generation/data/realtime-generation", body)
        return result.get("body", {}).get("hourlyGenerations", [])

    def get_realtime_consumption(self, start_date: str, end_date: str) -> list:
        """Gerçek zamanlı tüketim verisini çeker."""
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/consumption/data/realtime-consumption", body)
        return result.get("body", {}).get("hourlyConsumptions", [])

    def get_demand_forecast(self, start_date: str, end_date: str) -> list:
        """Yük tahmini verisini çeker."""
        body = {"startDate": start_date, "endDate": end_date}
        result = self._post("/v1/consumption/data/demand-forecast", body)
        return result.get("body", {}).get("loadEstimationPlanList", [])


# ── KULLANIM ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    client = EPIASClient()

    # Tek bir günün PTF verisini çek
    data = client.get_ptf(
        start_date="2024-01-01T00:00:00+03:00",
        end_date="2024-01-01T23:00:00+03:00",
    )

    for row in data:
        print(row)
