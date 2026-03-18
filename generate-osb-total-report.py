"""
Belirtilen musteriye ait gunluk KGUP tahmin Excel dosyasini okur, her tesis
icin saatlik reduksiyon degerlerini hesaplayarak ayri CSV dosyalari uretir
ve bu dosyalari YAML'da tanimli alicilara e-posta ile gonderir.

Kullanım:
  python generate-osb-total-report.py --customer sisecam
  python generate-osb-total-report.py --customer sisecam --date 2026-03-18
  python generate-osb-total-report.py --customer met --date 2026-03-18

Argümanlar:
  --customer  : Zorunlu. config/<customer>.yaml dosyasını okur. (örn: sisecam, met)
  --date      : Opsiyonel. YYYY-MM-DD formatında hedef tarih (end_date).
                Belirtilmezse yarın (today + 1) kullanılır.
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
    args = parser.parse_args()

    if args.date:
        try:
            end_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"Geçersiz tarih formatı: '{args.date}' — YYYY-MM-DD kullanın.")
    else:
        end_date = date.today() + timedelta(days=1)

    return args.customer, end_date


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
    else:
        meter_ids = [str(mid) for mid in facility_cfg["meter_ids"]]
        available = [col for col in meter_ids if col in day_df.columns]
        missing   = set(meter_ids) - set(available)
        if missing:
            print(f"  [WARN] {name}: Şu meter_id'ler Excel'de bulunamadı → {missing}", file=sys.stderr)
        miktar_series = -day_df[available].sum(axis=1) if available else pd.Series([0.0] * 24)

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
) -> None:
    smtp_server     = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port       = int(os.getenv("SMTP_PORT", 587))
    sender_email    = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD")

    if not sender_email or not sender_password:
        raise EnvironmentError("SENDER_EMAIL veya SENDER_PASSWORD .env dosyasında bulunamadı.")

    subject = f"KGÜP Tahmin Raporları - {report_date}"
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
# 6. Ana akış
# ─────────────────────────────────────────────
def main():
    customer, end_date = parse_args()
    start_date = end_date - timedelta(days=1)

    print(f"👤 Müşteri         : {customer}")
    print(f"📅 Çalışma tarihi  : {start_date}  →  Hedef gün: {end_date}")

    cfg = load_config(customer)

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

    # Çıktı dizini: teias-osb-raporlari/outputs/<customer>/csv_reports/YYYY-MM-DD/
    output_dir = Path(__file__).parent / "outputs" / customer / "csv_reports" / str(end_date)
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


if __name__ == "__main__":
    main()