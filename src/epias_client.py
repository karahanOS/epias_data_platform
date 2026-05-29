import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Union, List, Dict

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── SABİTLER ──────────────────────────────────────────────────────────────────
AUTH_URL        = "https://giris.epias.com.tr/cas/v1/tickets"
BASE_URL        = "https://seffaflik.epias.com.tr/electricity-service"
TGT_LIFETIME    = timedelta(hours=1, minutes=30)  # TTL 2 saat; güvenli tampon
REQUEST_TIMEOUT = 30    # saniye
MAX_RETRIES     = 3
RETRY_BACKOFF   = 2.0   # üstel geri çekilme çarpanı (2s → 4s → 8s)


class EPIASClient:
    """
    EPIAS electricity-service API istemcisi.

    Kullanım:
        client = EPIASClient()
        data   = client.get_ptf_smf_sdf("2025-01-01", "2025-01-01")
    """

    def __init__(self) -> None:
        self.username = os.getenv("EPIAS_USERNAME")
        self.password = os.getenv("EPIAS_PASSWORD")
        if not self.username or not self.password:
            raise EnvironmentError(
                "EPIAS_USERNAME ve EPIAS_PASSWORD .env dosyasında tanımlı olmalı."
            )
        self.logger = logger
        self._tgt: Optional[str]        = None
        self._token_time: Optional[datetime] = None

    # ── KİMLİK DOĞRULAMA ──────────────────────────────────────────────────────

    def _fetch_tgt(self) -> str:
        """CAS sunucusundan yeni TGT alır."""
        logger.info("EPIAS Auth: Yeni TGT alınıyor...")
        r = requests.post(
            AUTH_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain"},
            data={"username": self.username, "password": self.password},
            timeout=REQUEST_TIMEOUT,
        )
        if not r.ok:
            raise RuntimeError(f"TGT alınamadı — HTTP {r.status_code}: {r.text[:200]}")

        # Bazı CAS impl. response.text, bazıları Location header döner
        tgt = r.text.strip()
        if not tgt.startswith("TGT-"):
            loc = r.headers.get("Location", "")
            tgt = loc.split("/")[-1] if "TGT-" in loc else ""
        if not tgt:
            raise RuntimeError(f"TGT formatı tanımsız — response: {r.text[:200]}")

        logger.info(f"TGT alındı: {tgt[:20]}...")
        return tgt

    def _get_valid_tgt(self) -> str:
        """TGT önbelleği; süresi dolduysa yeniler."""
        now = datetime.now()
        if (
            self._tgt is None
            or self._token_time is None
            or (now - self._token_time) > TGT_LIFETIME
        ):
            self._tgt        = self._fetch_tgt()
            self._token_time = now
        return self._tgt


    # ── HTTP KATMANI ──────────────────────────────────────────────────────────

    def _post(self, endpoint: str, body: dict) -> dict:
        """
        POST isteği gönderir; 401'de TGT yeniler, 5xx'te exponential backoff ile yeniden dener.
        Başarı ve hata durumlarını loglar.
        """
        url      = f"{BASE_URL}{endpoint}"
        last_exc: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                r = requests.post(
                    url,
                    headers={
                        "TGT": self._get_valid_tgt(),
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=REQUEST_TIMEOUT,
                )

                if r.status_code == 401:
                    logger.warning(f"401 Unauthorized — TGT yenileniyor (deneme {attempt + 1})")
                    self._tgt = None
                    continue

                if not r.ok:
                    # ── EPİAŞ İŞ MANTIĞI (BUSINESS) HATALARINI YÖNETME ────────────────
                    if r.status_code == 400:
                        try:
                            error_json = r.json()
                            for err in error_json.get("errors", []):
                                error_code = err.get("errorCode", "")
                                error_msg = err.get("errorMessage", "").lower()
                                
                                # Eğer hata veri bulunamamasıyla ilgiliyse pipeline'ı kırma, boş dön
                                if "BUS" in error_code or "veri bulunmamaktadır" in error_msg:
                                    logger.warning(
                                        f"⚠️  EPİAŞ Veri Boşluğu ({endpoint}) — "
                                        f"Mesaj: {err.get('errorMessage')} -> Boş liste dönülüyor."
                                    )
                                    return {"items": []}
                        except Exception:
                            pass # JSON parse edilemezse loglayıp raise_for_status'a devam et

                    # Gerçek teknik hatalar (404, 500 vb.) için loglamaya ve çökmeye devam et
                    logger.error(
                        f"HTTP {r.status_code} — {endpoint}\n"
                        f"  payload : {json.dumps(body)}\n"
                        f"  response: {r.text[:500]}"
                    )
                    r.raise_for_status()

                result = r.json()

                # Response yapısını logla; items boşsa detay ver
                items = result.get("items") if isinstance(result, dict) else None
                if items:
                    logger.info(f"✅ {endpoint} — {len(items)} kayıt")
                else:
                    logger.warning(
                        f"⚠️  Boş response — {endpoint}\n"
                        f"  payload: {json.dumps(body)}\n"
                        f"  keys   : {list(result.keys()) if isinstance(result, dict) else type(result)}\n"
                        f"  body   : {str(result)[:400]}"
                    )
                return result

            except requests.exceptions.Timeout:
                last_exc = TimeoutError(f"Timeout ({REQUEST_TIMEOUT}s): {url}")
                logger.warning(f"Deneme {attempt + 1}/{MAX_RETRIES} — timeout")
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                logger.warning(f"Deneme {attempt + 1}/{MAX_RETRIES} — bağlantı hatası: {exc}")
            except requests.exceptions.HTTPError as exc:
                if r.status_code < 500:
                    raise  # 4xx → retry'dan fayda yok
                last_exc = exc
                logger.warning(f"Deneme {attempt + 1}/{MAX_RETRIES} — sunucu hatası {r.status_code}")

            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF ** (attempt + 1)
                logger.info(f"  {wait:.0f}s bekleniyor...")
                time.sleep(wait)

        raise RuntimeError(
            f"API {MAX_RETRIES} denemede başarısız: {url}\n"
            f"Son hata: {last_exc}"
        )

    # ── TARİH YARDIMCILARI ────────────────────────────────────────────────────

    @staticmethod
    def _to_iso(date_str: str, end_of_day: bool = False) -> str:
        """
        YYYY-MM-DD → ISO 8601 (Turkey +03:00)
        Swagger örneği: startDate=T00:00:00, endDate=T23:00:00
        """
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        suffix = "T23:00:00+03:00" if end_of_day else "T00:00:00+03:00"
        return dt.strftime(f"%Y-%m-%d{suffix}")

    def _date_body(self, start: str, end: str) -> dict:
        """Standart startDate / endDate payload'ı üretir."""
        return {
            "startDate": self._to_iso(start, end_of_day=False),
            "endDate":   self._to_iso(end,   end_of_day=True),
        }

    # ═════════════════════════════════════════════════════════════════════════
    # ENDPOINTler
    # ═════════════════════════════════════════════════════════════════════════

    # ── FİYAT ─────────────────────────────────────────────────────────────────

    def get_ptf_smf_sdf(self, start_date: str, end_date: str) -> list:
        """
        Piyasa Takas Fiyatı (PTF / MCP).
        NOT: /v1/data/ptf-smf-sdf electricity-service'te 404 (reporting-service altında).
             Karşılığı: POST /v1/markets/dam/data/mcp
        """
        return self._post(
            "/v1/markets/dam/data/mcp",
            self._date_body(start_date, end_date),
        ).get("items", [])

    # get_ptf_smf_sdf alias'ları — geriye dönük uyumluluk
    def get_mcp(self, start_date: str, end_date: str) -> list:
        return self.get_ptf_smf_sdf(start_date, end_date)

    def get_pricing_data(self, start_date: str, end_date: str) -> list:
        return self.get_ptf_smf_sdf(start_date, end_date)

    def get_smf(self, start_date: str, end_date: str) -> list:
        """Sistem Marjinal Fiyatı. POST /v1/markets/bpm/data/system-marginal-price"""
        data = self._post(
            "/v1/markets/bpm/data/system-marginal-price",
            self._date_body(start_date, end_date),
        ).get("items", [])
        for row in data:
            val = row.get("systemMarginalPrice")
            if val is not None:
                row["systemMarginalPrice"] = float(val)
        return data

    # ── GÖP ───────────────────────────────────────────────────────────────────

    def get_supply_demand(self, start_date: str, end_date: str) -> list:
        """
        Arz-talep eğrisi — PTF'nin neden tavan/minimum olduğunu açıklar.
        POST /v1/markets/dam/data/supply-demand
        NOT: Swagger DateRangeRequest gösteriyor; API {"date": "..."} istiyor.
        """
        return self._post(
            "/v1/markets/dam/data/supply-demand",
            {"date": self._to_iso(start_date, end_of_day=False)},
        ).get("items", [])

    def get_dam_clearing_quantity(self, start_date: str, end_date: str) -> list:
        """GÖP saatlik eşleşme miktarı. POST /v1/markets/dam/data/clearing-quantity"""
        return self._post(
            "/v1/markets/dam/data/clearing-quantity",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_dam_trade_volume(self, start_date: str, end_date: str) -> list:
        """GÖP işlem hacmi (TL). POST /v1/markets/dam/data/day-ahead-market-trade-volume"""
        return self._post(
            "/v1/markets/dam/data/day-ahead-market-trade-volume",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_submitted_bid_volume(self, start_date: str, end_date: str) -> list:
        """0 TL/MWh alış teklif hacmi (tedarik). POST /v1/markets/dam/data/submitted-bid-order-volume"""
        return self._post(
            "/v1/markets/dam/data/submitted-bid-order-volume",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_submitted_sales_volume(self, start_date: str, end_date: str) -> list:
        """Azami fiyat satış teklif hacmi (üretim). POST /v1/markets/dam/data/submitted-sales-order-volume"""
        return self._post(
            "/v1/markets/dam/data/submitted-sales-order-volume",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_price_independent_bid(self, start_date: str, end_date: str) -> list:
        """Fiyat bağımsız alış teklifleri. POST /v1/markets/dam/data/price-independent-bid"""
        return self._post(
            "/v1/markets/dam/data/price-independent-bid",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_price_independent_offer(self, start_date: str, end_date: str) -> list:
        """Fiyat bağımsız satış teklifleri. POST /v1/markets/dam/data/price-independent-offer"""
        return self._post(
            "/v1/markets/dam/data/price-independent-offer",
            self._date_body(start_date, end_date),
        ).get("items", [])

    # ── GİP ───────────────────────────────────────────────────────────────────

    def get_idm_transaction_history(self, start_date: str, end_date: str) -> list:
        """GİP anlık işlemler. POST /v1/markets/idm/data/transaction-history"""
        return self._post(
            "/v1/markets/idm/data/transaction-history",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_idm_matching_quantity(self, start_date: str, end_date: str) -> list:
        """GİP eşleşme miktarı saatlik/blok. POST /v1/markets/idm/data/matching-quantity"""
        return self._post(
            "/v1/markets/idm/data/matching-quantity",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_idm_weighted_average_price(self, start_date: str, end_date: str) -> list:
        """GİP ağırlıklı ortalama fiyat. POST /v1/markets/idm/data/weighted-average-price"""
        return self._post(
            "/v1/markets/idm/data/weighted-average-price",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_idm_bid_offer_quantities(self, start_date: str, end_date: str) -> list:
        """GİP alış/satış teklif hacimleri. POST /v1/markets/idm/data/bid-offer-quantities"""
        return self._post(
            "/v1/markets/idm/data/bid-offer-quantities",
            self._date_body(start_date, end_date),
        ).get("items", [])

    # ── DGP / SİSTEM ──────────────────────────────────────────────────────────

    def get_order_summary_up(self, start_date: str, end_date: str) -> list:
        """YAL (yük alma talimatı). POST /v1/markets/bpm/data/order-summary-up"""
        return self._post(
            "/v1/markets/bpm/data/order-summary-up",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_order_summary_down(self, start_date: str, end_date: str) -> list:
        """YAT (yük atma talimatı). POST /v1/markets/bpm/data/order-summary-down"""
        return self._post(
            "/v1/markets/bpm/data/order-summary-down",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_system_direction(self, start_date: str, end_date: str) -> list:
        """Sistem yönü saatlik. POST /v1/markets/bpm/data/system-direction"""
        return self._post(
            "/v1/markets/bpm/data/system-direction",
            self._date_body(start_date, end_date),
        ).get("items", [])

    # ── ÜRETİM ────────────────────────────────────────────────────────────────

    def get_realtime_generation(self, start_date: str, end_date: str) -> list:
        """
        Kaynak bazında saatlik gerçek zamanlı üretim.
        POST /v1/generation/data/realtime-generation
        Sayısal alanlar float'a normalize edilir.
        """
        data = self._post(
            "/v1/generation/data/realtime-generation",
            self._date_body(start_date, end_date),
        ).get("items", [])
        _NUMERIC = (
            "total", "naturalGas", "dammedHydro", "lignite", "river",
            "importCoal", "wind", "sun", "fueloil", "geothermal",
            "asphaltiteCoal", "blackCoal", "biomass", "naphta", "lng",
            "importExport", "wasteheat",
        )
        for row in data:
            for col in _NUMERIC:
                if row.get(col) is not None:
                    row[col] = float(row[col])
        return data

    def get_dpp(
        self,
        start_date: str,
        end_date: str,
        org_id: Optional[int] = None,
        uevcb_id: Optional[int] = None,
    ) -> list:
        """
        Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP).
        POST /v1/generation/data/dpp
        NOT: region zorunlu (swagger'da eksik). Türkiye tek bölge → "TR1".
        """
        body = self._date_body(start_date, end_date)
        body["region"] = "TR1"
        if org_id:
            body["organizationId"] = org_id
        if uevcb_id:
            body["uevcbId"] = uevcb_id
        return self._post("/v1/generation/data/dpp", body).get("items", [])

    def get_dpp_bulk(self, start_date: str, end_date: str) -> list:
        """Tüm UEVCB'ler toplu KGÜP. POST /v1/generation/data/dpp-bulk"""
        return self._post(
            "/v1/generation/data/dpp-bulk",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_injection_quantity(self, start_date: str, end_date: str) -> list:
        """Uzlaştırmaya esas veriş miktarı (UEVM). POST /v1/generation/data/injection-quantity"""
        return self._post(
            "/v1/generation/data/injection-quantity",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_aic(self, start_date: str, end_date: str) -> list:
        """
        Emre Amade Kapasite.
        POST /v1/generation/data/aic
        NOT: region zorunlu (swagger'da eksik) → "TR1".
        """
        body = self._date_body(start_date, end_date)
        body["region"] = "TR1"
        return self._post("/v1/generation/data/aic", body).get("items", [])

    def get_organization_list(self) -> list:
        """Organizasyon referans listesi. POST /v1/generation/data/organization-list"""
        return self._post("/v1/generation/data/organization-list", {}).get("items", [])

    def get_uevcb_list(self, start_date: str, end_date: str) -> list:
        self.logger.info("⚡ UEVCB listesi çekiliyor... (BULK ENDPOINT kullanılarak yüksek hızda)")
        import time
        
        # 1. Önce sistemdeki organizasyon (şirket) listesini al
        orgs = self.get_market_participants()
        org_ids = [org["id"] for org in orgs if "id" in org]
        
        all_uevcbs = []
        batch_size = 100 # Güvenlik için 100'erli paketler halinde gönderiyoruz
        
        self.logger.info(f"Toplam {len(org_ids)} şirket bulundu. 100'erli paketler halinde Bulk API'ye gönderiliyor...")

        # 2. Şirket ID'lerini paketlere (chunk) böl ve topluca gönder
        for i in range(0, len(org_ids), batch_size):
            chunk_ids = org_ids[i:i + batch_size]
            
            payload = {
                "startDate": f"{start_date}T00:00:00+03:00",
                "organizationIdList": chunk_ids  # Bulk endpoint'in beklediği liste parametresi
            }
            
            try:
                # DİKKAT: Eski uevcb-list yerine yeni uevcb-list-bulk kullanıyoruz!
                res = self._post("/v1/generation/data/uevcb-list-bulk", payload)
                items = res.get("items", [])
                all_uevcbs.extend(items)
                
            except Exception as e:
                self.logger.error(f"Bulk istek sırasında hata (Paket {i}-{i+batch_size}): {e}")
                
            # Rate limit'e (80 req/min) takılmamak için paketler arası çok kısa bir bekleme
            time.sleep(0.5)
            
        self.logger.info(f"✅ İşlem saniyeler içinde tamamlandı! Toplam {len(all_uevcbs)} adet UEVCB bulundu.")
        return all_uevcbs

    # ── TÜKETİM ───────────────────────────────────────────────────────────────

    def get_realtime_consumption(self, start_date: str, end_date: str) -> list:
        """Saatlik gerçek zamanlı tüketim. POST /v1/consumption/data/realtime-consumption"""
        return self._post(
            "/v1/consumption/data/realtime-consumption",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_load_estimation_plan(self, start_date: str, end_date: str) -> list:
        """Yük Tahmini Planı (LEP). POST /v1/consumption/data/load-estimation-plan"""
        data = self._post(
            "/v1/consumption/data/load-estimation-plan",
            self._date_body(start_date, end_date),
        ).get("items", [])
        for row in data:
            if row.get("lep") is not None:
                row["lep"] = float(row["lep"])
        return data

    # ── YENİLENEBİLİR / YEKDEM ───────────────────────────────────────────────

    def get_unlicensed_generation(self, start_date: str, end_date: str) -> list:
        """YEKDEM lisanssız üretim miktarı (aylık uzlaştırma, ~35g gecikme).
        POST /v1/renewables/data/unlicensed-generation-amount"""
        return self._post(
            "/v1/renewables/data/unlicensed-generation-amount",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_unlicensed_generation_cost(self, start_date: str, end_date: str) -> list:
        """Lisanssız üretim maliyeti (aylık). POST /v1/renewables/data/unlicensed-generation-cost"""
        return self._post(
            "/v1/renewables/data/unlicensed-generation-cost",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_licensed_realtime_generation(self, start_date: str, end_date: str) -> list:
        """Lisanslı YEKDEM üretimi. POST /v1/renewables/data/licensed-realtime-generation"""
        return self._post(
            "/v1/renewables/data/licensed-realtime-generation",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_res_generation_and_forecast(self, start_date: str, end_date: str) -> list:
        """İzlenebilen RES'ler üretim + tahmin. POST /v1/renewables/data/res-generation-and-forecast"""
        return self._post(
            "/v1/renewables/data/res-generation-and-forecast",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_generation_forecast(self, start_date: str, end_date: str) -> list:
        """
        YEKDEM üretim tahmini. POST /v1/renewables/data/generation-forecast
        """
        return self._post(
            "/v1/renewables/data/generation-forecast",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_yekdem_total_cost(self, start_date: str, end_date: str) -> list:
        """Toplam YEKDEM maliyeti (aylık). POST /v1/renewables/data/total-cost"""
        return self._post(
            "/v1/renewables/data/total-cost",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_yekdem_unit_cost(self, start_date: str, end_date: str) -> list:
        """Birim YEKDEM maliyeti (aylık). POST /v1/renewables/data/unit-cost"""
        return self._post(
            "/v1/renewables/data/unit-cost",
            self._date_body(start_date, end_date),
        ).get("items", [])

    def get_new_installed_capacity(self, start_date: str, end_date: str = None) -> list:
        """
        YEKDEM kapsamındaki yeni kurulu güç — lisanssız büyüme trendi.
        POST /v1/renewables/data/new-installed-capacity
        """
        try:
            # Airflow'dan gelen "YYYY-MM-DD" formatındaki tarihi ayın 1. gününe yuvarlıyoruz
            pure_date = start_date.split("T")[0]
            dt = datetime.strptime(pure_date, "%Y-%m-%d")
            first_day_of_month = dt.replace(day=1).strftime("%Y-%m-%d")
            
            # EPİAŞ'ın zorunlu tuttuğu ISO 8601 (+03:00) formatına dönüştürüyoruz
            period_val = self._to_iso(first_day_of_month)
        except Exception as e:
            logger.warning(f"Tarih formatlanırken hata oluştu, fallback uygulanıyor: {str(e)}")
            period_val = self._to_iso(datetime.now().replace(day=1).strftime("%Y-%m-%d"))

        body = {"period": period_val}
        
        logger.info(f"🚀 get_new_installed_capacity isteği gönderiliyor. Dönem: {period_val}")
        
        return self._post(
            "/v1/renewables/data/new-installed-capacity",
            body
        ).get("items", [])

    def get_renewables_participant(self, start_date: str, end_date: str) -> list:
        """
        YEKDEM katılımcı listesi (yıllık). POST /v1/renewables/data/renewables-participant
        NOT: Swagger DateRangeRequest; API {"year": YYYY} istiyor.
        """
        year = datetime.strptime(start_date, "%Y-%m-%d").year
        return self._post(
            "/v1/renewables/data/renewables-participant",
            {"year": year},
        ).get("items", [])

    # ── KATILIMCILAR ──────────────────────────────────────────────────────────

    def get_market_participants(self) -> list:
        """Piyasa katılımcıları (GÖP/GİP/VEP/YEK-G üyelikleri).
        POST /v1/markets/general-data/data/market-participants"""
        return self._post(
            "/v1/markets/general-data/data/market-participants", {}
        ).get("items", [])

    # ── DENGESİZLİK (HESAPLAMALI) ─────────────────────────────────────────────

    def get_imbalance_quantity(self, start_date: str, end_date: str) -> list:
        """
        Saatlik dengesizlik miktarı ve sistem yönü.

        Hesaplama: imbalance = generation.total − consumption
        Yön      : ENERGY_SURPLUS  (üretim fazlası)
                   ENERGY_DEFICIT  (tüketim fazlası)
                   BALANCED

        Neden hesaplamalı?
          EPIAS imbalance-quantity endpoint'i aylık uzlaştırma sonucunda yayınlanır (~35g gecikme).
          Gerçek zamanlı üretim ve tüketim T+2 saatte yayınlanır; anlık analiz için bu yeterli.
        """
        gen_items = self.get_realtime_generation(start_date, end_date)
        con_items = self.get_realtime_consumption(start_date, end_date)

        if not gen_items and not con_items:
            logger.warning(f"imbalance: her iki kaynak da boş ({start_date})")
            return []

        # Tüketim satırlarını tarih anahtarıyla sözlüğe al
        con_by_date: dict[str, float] = {}
        for row in con_items:
            dt  = row.get("date") or row.get("datetime") or row.get("hour")
            val = row.get("consumption") or row.get("tüketim") or row.get("amount") or 0.0
            if dt:
                con_by_date[dt] = float(val) if val is not None else 0.0

        result = []
        for gen in gen_items:
            dt          = gen.get("date") or gen.get("datetime") or gen.get("hour")
            generation  = float(gen.get("total") or 0.0)
            consumption = con_by_date.get(dt, 0.0)
            imbalance   = round(generation - consumption, 3)
            result.append({
                "date":              dt,
                "generationTotal":   generation,
                "consumption":       consumption,
                "imbalanceQuantity": imbalance,
                "systemDirection": (
                    "ENERGY_SURPLUS" if imbalance > 0
                    else "ENERGY_DEFICIT" if imbalance < 0
                    else "BALANCED"
                ),
            })

        logger.info(
            f"✅ imbalance hesaplandı — {len(result)} saat "
            f"(gen={len(gen_items)}, con={len(con_items)})"
        )
        return result


# ── KULLANIM ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = EPIASClient()
    rows = client.get_ptf_smf_sdf("2025-01-01", "2025-01-01")
    print(f"PTF kayıt sayısı: {len(rows)}")
    if rows:
        print("İlk kayıt:", rows[0])