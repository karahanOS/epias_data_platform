import os
import json
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# ── SABİTLER (Swagger v1.15.11'den doğrulandı) ───────────────────────────────
AUTH_URL     = "https://giris.epias.com.tr/cas/v1/tickets"
BASE_URL     = "https://seffaflik.epias.com.tr/electricity-service"
TGT_LIFETIME = timedelta(hours=1, minutes=30)

REQUEST_TIMEOUT = 30
MAX_RETRIES     = 3
RETRY_BACKOFF   = 2.0


class EPIASClient:
    def __init__(self):
        self.username = os.getenv("EPIAS_USERNAME")
        self.password = os.getenv("EPIAS_PASSWORD")
        if not self.username or not self.password:
            raise EnvironmentError(
                "EPIAS_USERNAME ve EPIAS_PASSWORD environment variable'ları tanımlı olmalı."
            )
        self._tgt        = None
        self._token_time = None

    # ── AUTH ──────────────────────────────────────────────────────────────────

    def _fetch_tgt(self) -> str:
        logger.info("EPIAS Auth: Yeni TGT alınıyor...")
        response = requests.post(
            AUTH_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain"},
            data={"username": self.username, "password": self.password},
            timeout=REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(f"TGT alınamadı! HTTP {response.status_code}: {response.text[:200]}")
        tgt = response.text.strip()
        if not tgt.startswith("TGT-"):
            loc = response.headers.get("Location", "")
            tgt = loc.split("/")[-1] if "TGT-" in loc else ""
        if not tgt:
            raise RuntimeError(f"TGT formatı geçersiz: {response.text[:200]}")
        logger.info(f"TGT alındı: {tgt[:20]}...")
        return tgt

    def _get_valid_tgt(self) -> str:
        now = datetime.now()
        if (self._tgt is None or self._token_time is None
                or (now - self._token_time) > TGT_LIFETIME):
            self._tgt        = self._fetch_tgt()
            self._token_time = now
        return self._tgt

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _post(self, endpoint: str, body: dict) -> dict:
        url      = f"{BASE_URL}{endpoint}"
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    url,
                    headers={"TGT": self._get_valid_tgt(), "Content-Type": "application/json"},
                    json=body,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 401:
                    logger.warning(f"401 — TGT yenileniyor (deneme {attempt + 1})")
                    self._tgt = None
                    continue

                if not response.ok:
                    logger.error(
                        f"HTTP {response.status_code} — {endpoint}\n"
                        f"Payload: {json.dumps(body)}\n"
                        f"Response: {response.text[:500]}"
                    )
                    response.raise_for_status()

                result = response.json()

                items = result.get("items") if isinstance(result, dict) else None
                if not items:
                    logger.warning(
                        f"⚠️  Boş/eksik 'items' — {endpoint}\n"
                        f"    Payload: {json.dumps(body)}\n"
                        f"    Keys: {list(result.keys()) if isinstance(result, dict) else type(result)}\n"
                        f"    Body: {str(result)[:400]}"
                    )
                else:
                    logger.info(f"✅ {endpoint} — {len(items)} kayıt")

                return result

            except requests.exceptions.Timeout:
                last_exc = TimeoutError(f"Timeout ({REQUEST_TIMEOUT}s): {url}")
                logger.warning(f"Deneme {attempt + 1}/{MAX_RETRIES} — timeout")
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                logger.warning(f"Deneme {attempt + 1}/{MAX_RETRIES} — bağlantı hatası: {exc}")
            except requests.exceptions.HTTPError as exc:
                if response.status_code < 500:
                    raise
                last_exc = exc
                logger.warning(f"Deneme {attempt + 1}/{MAX_RETRIES} — 5xx hatası")

            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF ** (attempt + 1)
                logger.info(f"  {wait:.0f}s bekleniyor...")
                time.sleep(wait)

        raise RuntimeError(f"API {MAX_RETRIES} denemede başarısız: {url}\nSon hata: {last_exc}")

    # ── TARİH YARDIMCISI ──────────────────────────────────────────────────────

    @staticmethod
    def _to_iso(date_str: str, end_of_day: bool = False) -> str:
        """YYYY-MM-DD → ISO 8601 (+03:00). endDate daima T23:00:00 olmalı."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if end_of_day:
            return dt.strftime("%Y-%m-%dT23:00:00+03:00")
        return dt.strftime("%Y-%m-%dT00:00:00+03:00")

    def _date_body(self, start: str, end: str) -> dict:
        return {
            "startDate": self._to_iso(start, end_of_day=False),
            "endDate":   self._to_iso(end,   end_of_day=True),
        }

    # =========================================================================
    # ── MEVCUT ENDPOINTler ────────────────────────────────────────────────────
    # =========================================================================

    # ── Fiyat ─────────────────────────────────────────────────────────────────

    def get_ptf_smf_sdf(self, start_date: str, end_date: str) -> list:
        """PTF, SMF, SDF saatlik değerleri. POST /v1/data/ptf-smf-sdf"""
        return self._post(
            "/v1/data/ptf-smf-sdf",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_pricing_data(self, start_date: str, end_date: str) -> list:
        """Geriye dönük uyumluluk alias'ı."""
        return self.get_ptf_smf_sdf(start_date, end_date)

    # ── Üretim ────────────────────────────────────────────────────────────────

    def get_realtime_generation(self, start_date: str, end_date: str) -> list:
        """Kaynak bazında saatlik gerçek zamanlı üretim."""
        data = self._post(
            "/v1/generation/data/realtime-generation",
            self._date_body(start_date, end_date)
        ).get("items", [])
        numeric_cols = [
            "total", "naturalGas", "dammedHydro", "lignite", "river",
            "importCoal", "wind", "sun", "fueloil", "geothermal",
            "asphaltiteCoal", "blackCoal", "biomass", "naphta", "lng",
            "importExport", "wasteheat",
        ]
        for row in data:
            for col in numeric_cols:
                if col in row and row[col] is not None:
                    row[col] = float(row[col])
        return data

    def get_dpp(self, start_date: str, end_date: str,
                org_id: int = None, uevcb_id: int = None) -> list:
        """KGÜP — Kesinleşmiş Gün Öncesi Üretim Planı. region=TR1 zorunlu."""
        body = self._date_body(start_date, end_date)
        body["region"] = "TR1"
        if org_id:
            body["organizationId"] = org_id
        if uevcb_id:
            body["uevcbId"] = uevcb_id
        return self._post("/v1/generation/data/dpp", body).get("items", [])

    def get_dpp_bulk(self, start_date: str, end_date: str) -> list:
        """Tüm UEVCB'lerin toplu KGÜP değerleri. POST /v1/generation/data/dpp-bulk"""
        return self._post(
            "/v1/generation/data/dpp-bulk",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_organization_list(self) -> list:
        """Organizasyon listesi."""
        return self._post("/v1/generation/data/organization-list", {}).get("items", [])

    def get_uevcb_list(self) -> list:
        """UEVCB listesi (organizationId ile filtreli çağrılabilir)."""
        return self._post("/v1/generation/data/uevcb-list", {}).get("items", [])

    # ── Tüketim ───────────────────────────────────────────────────────────────

    def get_realtime_consumption(self, start_date: str, end_date: str) -> list:
        """Saatlik gerçek zamanlı tüketim. T+2 saat gecikme."""
        return self._post(
            "/v1/consumption/data/realtime-consumption",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_load_estimation_plan(self, start_date: str, end_date: str) -> list:
        """Yük Tahmini Planı (LEP)."""
        data = self._post(
            "/v1/consumption/data/load-estimation-plan",
            self._date_body(start_date, end_date)
        ).get("items", [])
        for row in data:
            if "lep" in row and row["lep"] is not None:
                row["lep"] = float(row["lep"])
        return data

    # ── Dengesizlik (hesaplamalı) ──────────────────────────────────────────────

    def get_imbalance_quantity(self, start_date: str, end_date: str) -> list:
        """
        Dengesizlik miktarı ve sistem yönü — API değil, hesaplamayla üretilir.
        imbalance = realtime_generation.total − realtime_consumption
        T+2 saatte yayınlanan verilerden anlık sistem dengesi türetilir.
        """
        gen_items = self.get_realtime_generation(start_date, end_date)
        con_items = self.get_realtime_consumption(start_date, end_date)

        if not gen_items and not con_items:
            logger.warning(f"imbalance hesaplaması: üretim ve tüketim verisi boş ({start_date})")
            return []

        con_by_date = {}
        for row in con_items:
            dt  = row.get("date") or row.get("datetime") or row.get("hour")
            val = row.get("consumption") or row.get("tüketim") or row.get("amount") or 0.0
            if dt:
                con_by_date[dt] = float(val) if val is not None else 0.0

        result = []
        for gen_row in gen_items:
            dt          = gen_row.get("date") or gen_row.get("datetime") or gen_row.get("hour")
            generation  = float(gen_row.get("total") or 0.0)
            consumption = con_by_date.get(dt, 0.0)
            imbalance   = round(generation - consumption, 3)
            direction   = "ENERGY_SURPLUS" if imbalance > 0 else (
                          "ENERGY_DEFICIT" if imbalance < 0 else "BALANCED")
            result.append({
                "date":              dt,
                "generationTotal":   generation,
                "consumption":       consumption,
                "imbalanceQuantity": imbalance,
                "systemDirection":   direction,
            })

        logger.info(f"✅ imbalance hesaplandı: {len(result)} saat")
        return result

    # ── Katılımcılar ──────────────────────────────────────────────────────────

    def get_market_participants(self) -> list:
        """Piyasa katılımcıları — GÖP/GİP/VEP/YEK-G üyelikleri."""
        return self._post(
            "/v1/markets/general-data/data/market-participants", {}
        ).get("items", [])

    # ── Yenilenebilir / Lisanssız ─────────────────────────────────────────────

    def get_unlicensed_generation(self, start_date: str, end_date: str) -> list:
        """YEKDEM lisanssız üretim miktarı (kaynak bazında saatlik, T+35 gecikme)."""
        return self._post(
            "/v1/renewables/data/unlicensed-generation-amount",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_system_direction(self, start_date: str, end_date: str) -> list:
        """Sistem yönü (ENERGY_SURPLUS / ENERGY_DEFICIT) saatlik."""
        return self._post(
            "/v1/markets/bpm/data/system-direction",
            self._date_body(start_date, end_date)
        ).get("items", [])

    # =========================================================================
    # ── YENİ ENDPOINTler ──────────────────────────────────────────────────────
    # =========================================================================

    # ── GÖP: PTF oluşum analizi ───────────────────────────────────────────────

    def get_supply_demand(self, start_date: str, end_date: str) -> list:
        """
        GÖP arz-talep eğrisi — her fiyat kırılımındaki teklif miktarları.
        PTF'nin neden tavan/minimum olduğunu açıklar.
        POST /v1/markets/dam/data/supply-demand
        """
        return self._post(
            "/v1/markets/dam/data/supply-demand",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_mcp(self, start_date: str, end_date: str) -> list:
        """
        Piyasa Takas Fiyatı (PTF/MCP) — eşleşme miktarıyla birlikte.
        POST /v1/markets/dam/data/mcp
        """
        return self._post(
            "/v1/markets/dam/data/mcp",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_dam_clearing_quantity(self, start_date: str, end_date: str) -> list:
        """
        GÖP saatlik toplam eşleşme miktarı.
        POST /v1/markets/dam/data/clearing-quantity
        """
        return self._post(
            "/v1/markets/dam/data/clearing-quantity",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_dam_trade_volume(self, start_date: str, end_date: str) -> list:
        """
        GÖP saatlik işlem hacmi (TL bazında).
        POST /v1/markets/dam/data/day-ahead-market-trade-volume
        """
        return self._post(
            "/v1/markets/dam/data/day-ahead-market-trade-volume",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_submitted_bid_volume(self, start_date: str, end_date: str) -> list:
        """
        GÖP 0 TL/MWh'de sunulan alış (tüketim/tedarik) teklif hacmi.
        POST /v1/markets/dam/data/submitted-bid-order-volume
        """
        return self._post(
            "/v1/markets/dam/data/submitted-bid-order-volume",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_submitted_sales_volume(self, start_date: str, end_date: str) -> list:
        """
        GÖP azami fiyatta sunulan satış (üretim) teklif hacmi.
        POST /v1/markets/dam/data/submitted-sales-order-volume
        """
        return self._post(
            "/v1/markets/dam/data/submitted-sales-order-volume",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_price_independent_bid(self, start_date: str, end_date: str) -> list:
        """
        GÖP fiyat bağımsız alış teklifleri.
        Talebi bastırır → PTF düşer.
        POST /v1/markets/dam/data/price-independent-bid
        """
        return self._post(
            "/v1/markets/dam/data/price-independent-bid",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_price_independent_offer(self, start_date: str, end_date: str) -> list:
        """
        GÖP fiyat bağımsız satış teklifleri.
        Arzı baskılar → PTF yükselir.
        POST /v1/markets/dam/data/price-independent-offer
        """
        return self._post(
            "/v1/markets/dam/data/price-independent-offer",
            self._date_body(start_date, end_date)
        ).get("items", [])

    # ── GİP: Gün İçi Piyasası ─────────────────────────────────────────────────

    def get_idm_transaction_history(self, start_date: str, end_date: str) -> list:
        """
        GİP anlık işlem geçmişi — fiyat ve miktar.
        Hangi şirketin GİP kullandığını gösterir.
        POST /v1/markets/idm/data/transaction-history
        """
        return self._post(
            "/v1/markets/idm/data/transaction-history",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_idm_matching_quantity(self, start_date: str, end_date: str) -> list:
        """
        GİP saatlik/blok eşleşme miktarı.
        POST /v1/markets/idm/data/matching-quantity
        """
        return self._post(
            "/v1/markets/idm/data/matching-quantity",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_idm_weighted_average_price(self, start_date: str, end_date: str) -> list:
        """
        GİP saatlik ağırlıklı ortalama fiyat — PTF'den sapma analizi için.
        POST /v1/markets/idm/data/weighted-average-price
        """
        return self._post(
            "/v1/markets/idm/data/weighted-average-price",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_idm_bid_offer_quantities(self, start_date: str, end_date: str) -> list:
        """
        GİP toplam alış/satış teklif hacimleri — piyasa aktivitesi.
        POST /v1/markets/idm/data/bid-offer-quantities
        """
        return self._post(
            "/v1/markets/idm/data/bid-offer-quantities",
            self._date_body(start_date, end_date)
        ).get("items", [])

    # ── DGP: Dengeleme Güç Piyasası ───────────────────────────────────────────

    def get_smf(self, start_date: str, end_date: str) -> list:
        """
        Sistem Marjinal Fiyatı (SMF) ayrıntılı — DGP net talimat hacmiyle.
        PTF/SMF spread = dengesizlik maliyeti göstergesi.
        POST /v1/markets/bpm/data/system-marginal-price
        """
        return self._post(
            "/v1/markets/bpm/data/system-marginal-price",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_order_summary_up(self, start_date: str, end_date: str) -> list:
        """
        YAL — Yük Alma Talimatları (elektrik açığında devreye girenler).
        DGP'de hangi santralin çalıştığını gösterir.
        POST /v1/markets/bpm/data/order-summary-up
        """
        return self._post(
            "/v1/markets/bpm/data/order-summary-up",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_order_summary_down(self, start_date: str, end_date: str) -> list:
        """
        YAT — Yük Atma Talimatları (elektrik fazlasında azaltanlar).
        POST /v1/markets/bpm/data/order-summary-down
        """
        return self._post(
            "/v1/markets/bpm/data/order-summary-down",
            self._date_body(start_date, end_date)
        ).get("items", [])

    # ── Yenilenebilir / Lisanssız (genişletilmiş) ─────────────────────────────

    def get_res_generation_and_forecast(self, start_date: str, end_date: str) -> list:
        """
        RES üretim gerçekleşmesi ve tahmini (izlenebilen santraller).
        Hava kaynaklı üretim sapması → GİP aktivitesi korelasyonu için.
        POST /v1/renewables/data/res-generation-and-forecast
        """
        return self._post(
            "/v1/renewables/data/res-generation-and-forecast",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_generation_forecast(self, start_date: str, end_date: str) -> list:
        """
        YEKDEM portföyü üretim tahmini — öngörü hatası GİP etkisi analizi için.
        POST /v1/renewables/data/generation-forecast
        """
        return self._post(
            "/v1/renewables/data/generation-forecast",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_licensed_realtime_generation(self, start_date: str, end_date: str) -> list:
        """
        Lisanslı YEKDEM santral üretimi (kaynak bazında saatlik).
        Bölgesel yenilenebilir katkı karşılaştırması için.
        POST /v1/renewables/data/licensed-realtime-generation
        """
        return self._post(
            "/v1/renewables/data/licensed-realtime-generation",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_unlicensed_generation_cost(self, start_date: str, end_date: str) -> list:
        """
        Lisanssız üretim maliyeti (fatura dönemi) — YEKDEM yükü analizi.
        POST /v1/renewables/data/unlicensed-generation-cost
        Gecikme: ~35 gün (aylık fatura dönemi).
        """
        return self._post(
            "/v1/renewables/data/unlicensed-generation-cost",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_yekdem_total_cost(self, start_date: str, end_date: str) -> list:
        """
        Toplam YEKDEM maliyeti (lisanslı + lisanssız).
        POST /v1/renewables/data/total-cost
        Gecikme: ~35 gün.
        """
        return self._post(
            "/v1/renewables/data/total-cost",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_yekdem_unit_cost(self, start_date: str, end_date: str) -> list:
        """
        YEKDEM birim maliyeti (TL/MWh) — tüketiciye yansıma göstergesi.
        POST /v1/renewables/data/unit-cost
        Gecikme: ~35 gün.
        """
        return self._post(
            "/v1/renewables/data/unit-cost",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_renewables_participant(self, year: int = None) -> list:
        """
        YEKDEM katılımcı listesi — hangi şirketlerin dahil olduğu.
        POST /v1/renewables/data/renewables-participant
        """
        body = {"year": year} if year else {}
        return self._post(
            "/v1/renewables/data/renewables-participant", body
        ).get("items", [])

    def get_new_installed_capacity(self, start_date: str, end_date: str) -> list:
        """
        YEKDEM kapsamındaki yeni kurulu güç — lisanssız büyüme trendi.
        POST /v1/renewables/data/new-installed-capacity
        """
        return self._post(
            "/v1/renewables/data/new-installed-capacity",
            self._date_body(start_date, end_date)
        ).get("items", [])

    # ── Üretim (genişletilmiş) ────────────────────────────────────────────────

    def get_injection_quantity(self, start_date: str, end_date: str) -> list:
        """
        Uzlaştırmaya Esas Veriş Miktarı (UEVM) — fiili gerçekleşen üretim.
        KGÜP'ten sapma analizi için DPP ile karşılaştırılır.
        POST /v1/generation/data/injection-quantity
        """
        return self._post(
            "/v1/generation/data/injection-quantity",
            self._date_body(start_date, end_date)
        ).get("items", [])

    def get_aic(self, start_date: str, end_date: str) -> list:
        """
        Emre Amade Kapasite (EAK) — teorik max kapasite.
        EAK vs KGÜP farkı = kullanılmayan kapasite analizi.
        POST /v1/generation/data/aic
        """
        return self._post(
            "/v1/generation/data/aic",
            self._date_body(start_date, end_date)
        ).get("items", [])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = EPIASClient()
    data = client.get_ptf_smf_sdf("2024-01-01", "2024-01-01")
    print(f"PTF kayıt sayısı: {len(data)}")
    if data:
        print("İlk kayıt:", data[0])