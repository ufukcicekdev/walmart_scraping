# 🛒 Walmart Async Scraper

Walmart ürünlerini hızlı bir şekilde scrape eden async Python aracı.

## 📦 Kurulum

```bash
# Virtual environment oluştur
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Dependencies yükle
pip install -r requirements.txt
```

## 🚀 Kullanım

```bash
python walmart_async.py
```

## ⚙️ URL Ekleme

`walmart_async.py` dosyasını açın ve `urls` array'ine yeni kategoriler ekleyin:

```python
urls = [
    # Mevcut URL'ler...
    
    # Yeni kategori eklemek için:
    "https://www.walmart.com/browse/your-category-url",
]
```

## 📁 Çıktılar

- `scraped_data_async/all_products.json` - Tüm ürünler
- `scraped_data_async/product_details/` - Detaylı ürün bilgileri
- `scraped_data_async/kategori_adi/` - Kategori bazlı veriler

## ⚡ Hız

~6 ürün/saniye (26x geleneksel yöntemlerden hızlı)
