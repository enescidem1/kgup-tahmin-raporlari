"""
Belirtilen musteriye ait gunluk KGUP tahmin Excel dosyasini okur, her tesis
icin saatlik reduksiyon degerlerini hesaplayarak ayri CSV dosyalari uretir
ve bu dosyalari YAML'da tanimli alicilara e-posta ile gonderir.

Ek olarak (paralel akış): DB'deki predictions_buy_with_reduction tablosundan
aynı hesabı yaparak kontrol amaçlı CSV üretir ve db_recipients'a mail gönderir.

Kullanım:
  python generate-osb-total-report.py --customer sisecam
  python generate-osb-total-report.py --customer sisecam --date 2026-03-18
  python generate-osb-total-report.py --customer met --date 2026-03-18
  python generate-osb-total-report.py --customer sisecam --only-db
  python generate-osb-total-report.py --customer sisecam --date 2026-03-18 --only-db

Argümanlar:
  --customer  : Zorunlu. config/<customer>.yaml dosyasını okur. (örn: sisecam, met)
  --date      : Opsiyonel. YYYY-MM-DD formatında hedef tarih (end_date).
                Belirtilmezse yarın (today + 1) kullanılır.
  --only-db   : Sadece DB akışını çalıştırır (Excel akışı atlanır).
"""

import argparse
import mimetypes
import os
import smtplib
import sys
import yaml
import pandas as pd
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv

# .env script ile aynı seviyede (teias-osb-raporlari/.env)
load_dotenv(Path(__file__).parent / ".env")

# ─────────────────────────────────────────────
# 1. Argümanlar
# ─────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="OSB günlük KGÜP tahmin raporu üretici",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--customer",
        required=True,
        metavar="CUSTOMER",
        help=(
            "Müşteri adı. config/<customer>.yaml dosyasını okur.\n"
            "Örnek: --customer sisecam"
        ),
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help=(
            "Hedef tarih (end_date). Belirtilmezse yarın kullanılır.\n"
            "Örnek: --date 2026-03-18"
        ),
    )
    parser.add_argument(
        "--only-db",
        action="store_true",
        default=False,
        help=(
            "Sadece DB doğrulama akışını çalıştırır.\n"
            "Bu flag olmadan hem Excel hem DB akışı çalışır."
        ),
    )
    args = parser.parse_args()

    if args.date:
        try:
            end_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"Geçersiz tarih formatı: '{args.date}' — YYYY-MM-DD kullanın.")
    else:
        end_date = date.today() + timedelta(days=1)

    return args.customer, end_date, args.only_db


# ─────────────────────────────────────────────
# 2. Config yükle
# ─────────────────────────────────────────────
def load_config(customer: str) -> dict:
    config_path = Path(__file__).parent / "config" / f"{customer}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config bulunamadı: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────
# 3. Excel dosyasını bul ve oku
# ─────────────────────────────────────────────
def resolve_excel_path(base_path: str, start_date: date, end_date: date) -> Path:
    filename = f"Sisecam_all_meter_predcition_reduction_{start_date}_to_{end_date}.xlsx"
    path = Path(base_path) / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Prediction Excel bulunamadı: {path}\n"
            f"Beklenen dosya adı: {filename}"
        )
    return path


def _filter_and_sort(df: pd.DataFrame, target_date: date, sheet_name: str) -> pd.DataFrame:
    target_str = target_date.strftime("%d.%m.%Y")
    day_df = df[df["date"].astype(str) == target_str].copy()
    if len(day_df) != 24:
        raise ValueError(
            f"[{sheet_name}] Hedef tarih ({target_str}) için 24 satır bekleniyor, "
            f"{len(day_df)} satır bulundu."
        )
    day_df["_hour_int"] = (
        day_df["hour"].astype(str).str.replace(",", ".").str.split(".").str[0].astype(int)
    )
    return day_df.sort_values("_hour_int").reset_index(drop=True)


def load_target_day(excel_path: Path, target_date: date) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name="Predcition Data (Adjusted)")
    return _filter_and_sort(df, target_date, "Predcition Data (Adjusted)")


def load_toplam_day(excel_path: Path, target_date: date) -> pd.Series:
    df = pd.read_excel(excel_path, sheet_name="Toplam (Children Hariç)")
    day_df = _filter_and_sort(df, target_date, "Toplam (Children Hariç)")
    return day_df["total_prediction_value"].reset_index(drop=True)


# ─────────────────────────────────────────────
# 4. Facility CSV üret
# ─────────────────────────────────────────────
CSV_COLUMNS = [
    "Saat",
    "Miktar",
    "ic_tuketim",
    "kaynak_yetersizligi",
    "iklim_verim",
    "mucbir",
    "dissal",
    "guvenlik",
]


def build_facility_csv(
    day_df: pd.DataFrame,
    facility_cfg: dict,
    output_dir: Path,
    toplam_series: pd.Series = None,
    all_meter_ids: list = None,
) -> Path:
    order       = facility_cfg["order"][0]
    name        = facility_cfg["name"][0]
    facility_id = facility_cfg["facility_id"][0]

    if "meter_ids" not in facility_cfg:
        # toplam facility: sheet2 total - tüm meter toplamı
        all_available = [col for col in all_meter_ids if col in day_df.columns]
        meters_total  = (
            day_df[all_available].sum(axis=1).reset_index(drop=True)
            if all_available else pd.Series([0.0] * 24)
        )
        miktar_series = -(toplam_series - meters_total)
        miktar_series = miktar_series.clip(upper=0)  # pozitif değerleri 0'a çek
    else:
        meter_ids = [str(mid) for mid in facility_cfg["meter_ids"]]
        available = [col for col in meter_ids if col in day_df.columns]
        missing   = set(meter_ids) - set(available)
        if missing:
            print(f"  [WARN] {name}: Şu meter_id'ler Excel'de bulunamadı → {missing}", file=sys.stderr)
        miktar_series = -day_df[available].sum(axis=1) if available else pd.Series([0.0] * 24)
        miktar_series = miktar_series.clip(upper=0)  # pozitif değerleri 0'a çek

    out_df = pd.DataFrame(
        {
            "Saat":                day_df["_hour_int"].values,
            "Miktar":              miktar_series.values,
            "ic_tuketim":          "",
            "kaynak_yetersizligi": "",
            "iklim_verim":         "",
            "mucbir":              "",
            "dissal":              "",
            "guvenlik":            "",
        }
    )[CSV_COLUMNS]

    filename = f"{order}. {name} {facility_id}.csv"
    out_path  = output_dir / filename
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


# ─────────────────────────────────────────────
# 5. Mail gönder
# ─────────────────────────────────────────────
def send_report_email(
    recipients: list[str],
    report_date: date,
    attachment_paths: list[Path],
    subject_prefix: str = "",
) -> None:
    smtp_server     = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port       = int(os.getenv("SMTP_PORT", 587))
    sender_email    = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD")

    if not sender_email or not sender_password:
        raise EnvironmentError("SENDER_EMAIL veya SENDER_PASSWORD .env dosyasında bulunamadı.")

    prefix  = f"[{subject_prefix}] " if subject_prefix else ""
    subject = f"{prefix}KGÜP Tahmin Raporları - {report_date}"
    body = (
        f"Merhaba,\n\n"
        f"{report_date} tarihine ait KGÜP tahmin raporları ektedir.\n\n"
        f"İyi günler!"
    )

    msg = EmailMessage()
    msg["From"]    = sender_email
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)

    for path in attachment_paths:
        if not path.exists():
            print(f"  [WARN] Ek bulunamadı, atlandı: {path}", file=sys.stderr)
            continue
        mime_type, _ = mimetypes.guess_type(str(path))
        maintype, subtype = mime_type.split("/") if mime_type else ("application", "octet-stream")
        with open(path, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=path.name)
        print(f"  📎 Ek eklendi: {path.name}")

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)

    print(f"\n✉️  Mail {len(recipients)} alıcıya gönderildi: {', '.join(recipients)}")


# ─────────────────────────────────────────────
# 6. DB akışı — yeni paralel kontrol akışı
# ─────────────────────────────────────────────
def load_db_day(target_date: date, meter_ids: list[str]) -> pd.DataFrame:
    """
    predictions_buy_with_reduction tablosundan hedef güne ait satırları çeker.

    prediction_date kolonu timestamp formatındadır (örn: 2026-04-25 23:00:00.000).
    Hedef günün tüm saatlerini (00:00 → 23:00 arası 24 satır) çekeriz.

    Dönen DataFrame kolonları: meter_id, hour (0–23), prediction_value
    """
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 yüklü değil. 'pip install psycopg2-binary' ile yükleyin."
        )

    db_host     = os.getenv("DB_HOST")
    db_port     = os.getenv("DB_PORT", "5432")
    db_name     = os.getenv("DB_NAME")
    db_user     = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    missing_env = [k for k, v in {
        "DB_HOST": db_host, "DB_NAME": db_name,
        "DB_USER": db_user, "DB_PASSWORD": db_password,
    }.items() if not v]
    if missing_env:
        raise EnvironmentError(
            f"Şu DB ortam değişkenleri .env'de tanımlı değil: {', '.join(missing_env)}"
        )

    # Hedef günün UTC aralığı: date 00:00:00 → date+1 00:00:00
    # prediction_date timestamp'i yerel saat olarak varsayıyoruz.
    # Örnek: 2026-04-25 23:00:00 → saat 23 demek (0-indexed: hour=23)
    date_start = datetime.combine(target_date, datetime.min.time())
    date_end   = date_start + timedelta(days=1)

    placeholders = ", ".join(["%s"] * len(meter_ids))
    query = f"""
        SELECT
            meter_id,
            EXTRACT(HOUR FROM prediction_date)::int AS hour,
            prediction_value
        FROM predictions_buy_with_reduction
        WHERE
            prediction_date >= %s
            AND prediction_date < %s
            AND meter_id IN ({placeholders})
        ORDER BY meter_id, hour
    """

    params = [date_start, date_end] + meter_ids

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name,
        user=db_user, password=db_password,
    )
    try:
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    return df


def build_facility_csv_from_db(
    db_df: pd.DataFrame,
    facility_cfg: dict,
    output_dir: Path,
    all_meter_ids: list[str],
    toplam_by_hour: pd.Series = None,
) -> Path:
    """
    DB'den gelen pivot DataFrame'den tek bir tesis için CSV üretir.
    Mantık build_facility_csv ile birebir aynıdır.
    """
    order       = facility_cfg["order"][0]
    name        = facility_cfg["name"][0]
    facility_id = facility_cfg["facility_id"][0]

    # Pivot: satır=hour (0-23), sütun=meter_id, değer=prediction_value
    pivot = db_df.pivot_table(
        index="hour", columns="meter_id", values="prediction_value", aggfunc="sum"
    ).reindex(range(24), fill_value=0.0)
    pivot.columns = [str(c) for c in pivot.columns]

    hours = list(range(24))

    if "meter_ids" not in facility_cfg:
        # Toplam tesis: toplam_by_hour - tüm meter toplamı
        avail_cols    = [c for c in all_meter_ids if c in pivot.columns]
        meters_total  = pivot[avail_cols].sum(axis=1) if avail_cols else pd.Series([0.0] * 24, index=hours)
        if toplam_by_hour is None:
            raise ValueError("Toplam tesis için toplam_by_hour verisi gerekli ama DB'de bulunamadı.")
        miktar_series = -(toplam_by_hour - meters_total)
        miktar_series = miktar_series.clip(upper=0)
    else:
        meter_ids = [str(mid) for mid in facility_cfg["meter_ids"]]
        avail     = [c for c in meter_ids if c in pivot.columns]
        missing   = set(meter_ids) - set(avail)
        if missing:
            print(f"  [WARN][DB] {name}: Şu meter_id'ler DB'de bulunamadı → {missing}", file=sys.stderr)
        miktar_series = -pivot[avail].sum(axis=1) if avail else pd.Series([0.0] * 24, index=hours)
        miktar_series = miktar_series.clip(upper=0)

    out_df = pd.DataFrame(
        {
            "Saat":                hours,
            "Miktar":              miktar_series.values,
            "ic_tuketim":          "",
            "kaynak_yetersizligi": "",
            "iklim_verim":         "",
            "mucbir":              "",
            "dissal":              "",
            "guvenlik":            "",
        }
    )[CSV_COLUMNS]

    filename = f"{order}. {name} {facility_id}.csv"
    out_path  = output_dir / filename
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def run_db_flow(cfg: dict, end_date: date, customer: str) -> None:
    """
    DB'den veri çekip CSV üretir ve db_recipients'a mail gönderir.
    Bu akış tamamen bağımsızdır; mevcut Excel akışını etkilemez.
    """
    db_recipients = cfg.get("db_recipients", [])
    if not db_recipients:
        print("\n[DB SKIP] YAML'da db_recipients tanımlı değil, DB akışı atlandı.")
        return

    facilities    = cfg.get("facility", {})
    all_meter_ids = [
        str(mid)
        for fac in facilities.values()
        for mid in fac.get("meter_ids", [])
    ]

    print(f"\n{'─'*50}")
    print(f"🗄️  DB akışı başlıyor — hedef gün: {end_date}")

    try:
        db_df = load_db_day(end_date, all_meter_ids)
    except Exception as exc:
        print(f"  [ERROR] DB'den veri çekilemedi: {exc}", file=sys.stderr)
        return

    if db_df.empty:
        print(f"  [WARN] DB'de {end_date} için hiç veri bulunamadı.", file=sys.stderr)
        return

    row_count = len(db_df)
    print(f"  ✅ DB'den {row_count} satır çekildi.")

    # Toplam tesis varsa DB'deki toplam değeri hesapla
    # (toplam meter_id'si olmayan facility için)
    has_toplam_facility = any(
        "meter_ids" not in fac for fac in facilities.values()
    )
    toplam_by_hour = None
    if has_toplam_facility:
        # Tüm meter'ların saatlik toplamı (pivot üzerinden)
        pivot_all = db_df.pivot_table(
            index="hour", columns="meter_id", values="prediction_value", aggfunc="sum"
        ).reindex(range(24), fill_value=0.0)
        pivot_all.columns = [str(c) for c in pivot_all.columns]
        toplam_by_hour = pivot_all.sum(axis=1)
        # Not: Excel akışında Toplam sheet ayrı bir total içeriyordu.
        # DB'de ayrı bir "toplam" satırı yoksa meter toplamını kullanıyoruz.
        # Eğer DB'de ayrı bir toplam kolonu/satırı varsa burası güncellenmeli.

    output_dir = (
        Path(__file__).parent / "outputs" / customer / "csv_reports_db" / str(end_date)
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_csvs: list[Path] = []
    for key, fac in facilities.items():
        try:
            out_path = build_facility_csv_from_db(
                db_df, fac, output_dir,
                all_meter_ids=all_meter_ids,
                toplam_by_hour=toplam_by_hour,
            )
            generated_csvs.append(out_path)
            print(f"  ✔  [DB] {out_path.name}")
        except Exception as exc:
            print(f"  [ERROR] {fac.get('name', [key])[0]} CSV üretilemedi: {exc}", file=sys.stderr)

    if not generated_csvs:
        print("  [WARN] Hiç CSV üretilemedi, mail gönderilmedi.", file=sys.stderr)
        return

    print(f"\n  📁 DB CSV'leri kaydedildi → {output_dir}")
    print(f"\n  📨 DB kontrol maili gönderiliyor...")
    generated_csvs.sort(key=lambda p: p.name)
    send_report_email(db_recipients, end_date, generated_csvs, subject_prefix="Katsayı DB Kontrol")
    print(f"{'─'*50}\n")


# ─────────────────────────────────────────────
# 7. Ana akış
# ─────────────────────────────────────────────
def main():
    customer, end_date, only_db = parse_args()
    start_date = end_date - timedelta(days=1)

    print(f"👤 Müşteri         : {customer}")
    print(f"📅 Çalışma tarihi  : {start_date}  →  Hedef gün: {end_date}")
    if only_db:
        print(f"⚙️  Mod             : Sadece DB akışı (--only-db)")
    else:
        print(f"⚙️  Mod             : Excel + DB akışı")

    cfg = load_config(customer)

    # ── Excel akışı (--only-db yoksa) ──────────────────────────────────
    if not only_db:
        excel_path    = resolve_excel_path(cfg["path"], start_date, end_date)
        print(f"📂 Excel           : {excel_path}")

        day_df        = load_target_day(excel_path, end_date)
        toplam_series = load_toplam_day(excel_path, end_date)
        print(f"✅ {end_date} için veriler yüklendi.\n")

        facilities    = cfg.get("facility", {})
        all_meter_ids = [
            str(mid)
            for fac in facilities.values()
            for mid in fac.get("meter_ids", [])
        ]

        output_dir = (
            Path(__file__).parent / "outputs" / customer / "csv_reports" / str(end_date)
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        generated_csvs: list[Path] = []
        for key, fac in facilities.items():
            out_path = build_facility_csv(
                day_df, fac, output_dir,
                toplam_series=toplam_series,
                all_meter_ids=all_meter_ids,
            )
            generated_csvs.append(out_path)
            print(f"  ✔  {out_path.name}")

        print(f"\n📁 Dosyalar kaydedildi → {output_dir}")

        recipients = cfg.get("recipients", [])
        if recipients:
            print("\n📨 Mail gönderiliyor...")
            generated_csvs.sort(key=lambda p: p.name)
            send_report_email(recipients, end_date, generated_csvs)
        else:
            print("\n[SKIP] YAML'da recipients tanımlı değil, mail gönderilmedi.")

    # ── DB akışı (her zaman çalışır, --only-db yoksa da) ───────────────
    run_db_flow(cfg, end_date, customer)


if __name__ == "__main__":
    main()