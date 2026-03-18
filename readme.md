# KGÜP Tahmin Raporları

Günlük KGÜP tahmin Excel dosyalarından tesis bazlı CSV raporları üretir ve ilgili alıcılara e-posta ile gönderir.

## Kurulum

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install pyyaml pandas openpyxl python-dotenv
```

## Klasör Yapısı

```
teias-osb-raporlari/
├── config/
│   ├── sisecam.yaml
│   └── met.yaml
├── outputs/
│   ├── sisecam/
│   │   └── csv_reports/YYYY-MM-DD/
│   └── met/
├── .env
└── generate-osb-total-report.py
```

## Yapılandırma

### .env

```env
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your_email@domain.com
SENDER_PASSWORD=your_app_password_here
```

> Gmail kullanıyorsanız `SENDER_PASSWORD` için normal şifre değil, [App Password](https://myaccount.google.com/apppasswords) oluşturmanız gerekir.

### config/\<customer\>.yaml

```yaml
path: "/path/to/excel/files"
recipients:
  - "ornek@domain.com"
facility:
  toplam:
    name: ["EAK TOPLAM"]
    facility_id: [3192979]
    order: [1]
  ivedik:
    name: ["EAK İVEDİK"]
    facility_id: [5000219]
    order: [2]
    meter_ids: [739168, 739184]
```

`meter_ids` tanımlı olmayan facility (`toplam`) için değer şu formülle hesaplanır:

```
Miktar = -(sheet2_total - tüm_meter_id_toplamı)
```

Diğer tesisler için:

```
Miktar = -(ilgili meter_id'lerin saatlik toplamı)
```

## Kullanım

```bash
# Yarın için otomatik çalıştır
python generate-osb-total-report.py --customer sisecam

# Belirli bir tarih için
python generate-osb-total-report.py --customer sisecam --date 2026-03-18

# Farklı müşteri
python generate-osb-total-report.py --customer met --date 2026-03-18
```

| Argüman | Zorunlu | Açıklama |
|---|---|---|
| `--customer` | ✅ | `config/<customer>.yaml` dosyasını okur |
| `--date` | ❌ | Hedef tarih `YYYY-MM-DD`. Belirtilmezse yarın kullanılır |

## Çıktı

`outputs/<customer>/csv_reports/<YYYY-MM-DD>/` altında her tesis için:

```
1. EAK TOPLAM 3192979.csv
2. EAK İVEDİK 5000219.csv
3. EAK YENİŞEHİR 5000028.csv
...
```

CSV kolon düzeni: `Saat, Miktar, ic_tuketim, kaynak_yetersizligi, iklim_verim, mucbir, dissal, guvenlik`

Üretilen tüm CSV'ler YAML'daki `recipients` listesine tek bir e-posta ile gönderilir:

> **Konu:** `KGÜP Tahmin Raporları - 2026-03-18`