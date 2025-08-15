import aiohttp
import asyncio
import json
import re
import os
import time
import random
from typing import List, Dict, Optional

# FULL URL Array - PRODUCTION MODE (TÜM URL'LER AKTİF)
urls = [
    # Pizza & Pasta categories
    "https://www.walmart.com/browse/pizza-%26-pasta/0?_refineresult=true&_be_shelf_id=1470797&search_sort=100&facet=shelf_id%3A1470797&povid=976759_HubSpoke_5681029_Whatsfordinnertonight_Pizzaandpasta_Rweb_May_02&seo=pizza-%26-pasta&seo=0&page=1&affinityOverride=default",
    #"https://www.walmart.com/browse/pizza-%26-pasta/0?_refineresult=true&_be_shelf_id=1470797&search_sort=100&facet=shelf_id%3A1470797&povid=976759_HubSpoke_5681029_Whatsfordinnertonight_Pizzaandpasta_Rweb_May_02&seo=pizza-%26-pasta&seo=0&page=2&affinityOverride=default",
    #"https://www.walmart.com/browse/pizza-%26-pasta/0?_refineresult=true&_be_shelf_id=1470797&search_sort=100&facet=shelf_id%3A1470797&povid=976759_HubSpoke_5681029_Whatsfordinnertonight_Pizzaandpasta_Rweb_May_02&seo=pizza-%26-pasta&seo=0&page=3&affinityOverride=default",
    
    # Frozen Pizza
    #"https://www.walmart.com/browse/food/frozen-pizza/976759_976791_2072073?facet=brand%3Abettergoods&povid=976759_976791_NavPill_FreshPizzaAndPasta_bettergoodsFrozenPizza_Rweb_May_15_25",
    
    # Back to School Tech
    #"https://www.walmart.com/shop/back-to-school/tech?catId=3944_133251_1095191_1230614&povid=ETS_campaigns_BTS_facets_btstech_kidsheadphones",
   # "https://www.walmart.com/shop/back-to-school/tech?catId=3944_133251_1095191_1230614&povid=ETS_campaigns_BTS_facets_btstech_kidsheadphones&seo=back-to-school&seo=tech&page=2&affinityOverride=default",
    #"https://www.walmart.com/shop/back-to-school/tech?catId=3944_1229723&povid=ETS_campaigns_BTS_facets_btstech_wearables",
    
    # School Deals
    #"https://www.walmart.com/shop/deals/school?povid=XCATNav_SeasonalStyle_BTS_MiddleSchool_B_Pills_FY26_Schoolsavings",
    #"https://www.walmart.com/shop/deals/school?povid=XCATNav_SeasonalStyle_BTS_MiddleSchool_B_Pills_FY26_Schoolsavings&seo=deals&seo=school&page=2&affinityOverride=default",
    #"https://www.walmart.com/shop/deals/school?povid=XCATNav_SeasonalStyle_BTS_MiddleSchool_B_Pills_FY26_Schoolsavings&seo=deals&seo=school&page=3&affinityOverride=default",
    
    # Test URL (infant car seats)
    #"https://www.walmart.com/browse/baby/infant-car-seats/5427_91365_1101436?povid=baby_carseatscp_navpill_infantcarseats"
]

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en,en-US;q=0.9,tr-TR;q=0.8,tr;q=0.7',
    'cache-control': 'no-cache',
    'downlink': '10',
    'dpr': '2',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'Cookie': 'isoLoc=TR__t3; pxcts=fe1912c8-79fe-11f0-be49-e6c1de895bc1; _pxvid=fe1900a2-79fe-11f0-be48-2a3f977f810c; vtc=Soluu_01eo5X0b0XMyC_3M; bstc=Soluu_01eo5X0b0XMyC_3M; adblocked=false; ACID=9be6e35e-18cf-4b01-b9ac-26deef614a1f; _m=9; auth=MTAyOTYyMDE4WnM9h3kGV8PD95h6QBHAiLBKr47E750gvIEQB8ft%2FLgTyqXTFwnZqY9tEC9Cp9tYBQ2hxi22S9GtNp%2FARmhuRun2KaPuFGZWaOCto4e3%2B%2FpSPSkvt13fBLsbwgTIlooV767wuZloTfhm7Wk2KcjygmNzsF5Ho8U7SJCh0TVScOmefKXxSaW3AyXidyjqzyn%2F%2FL5CHLhBA1h%2FuShA5pYQr3OXhoK3UKlulqKb2DbATbsUMk70P8glgOEpLOprhDfMWpzMbgzyqWg6MoSOREDGWtBVuKL4bsXprFEzy8R5fMuquO%2F%2Bdho2xonuXw%2BO%2B0FhJ08gKLv3XCBLPs%2F6EhcupGYEPj5PHFMQnFQ1TnaYEQVzh5HW08LtY23%2BqVRMhqf6CzPXuDNDNQeYd%2B7YBFHXwJE5WBBdZBCyKnCQAR7o6eg%3D; hasACID=true; abqme=true; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; _pxhd=4aaaa4028a78869bb3796940b4078edd482f42f9fb337ea216ac9eb284ac4b65:fe1900a2-79fe-11f0-be48-2a3f977f810c; io_id=7ecb75a4-83e6-4c77-8244-33a5f5ad7544; _intlbu=false; _shcc=US; assortmentStoreId=3081; hasLocData=1; AID=wmlspartner=0:reflectorid=0000000000000000000000:lastupd=1755279776741; userAppVersion=usweb-1.219.0-ad514ef4d92733cbc19801ad805d4bcb9fc020f7-8141604r'
}

def extract_category_name(url: str) -> str:
    """URL'den kategori adı çıkar"""
    if "pizza" in url.lower():
        if "page=1" in url or ("page=" not in url and "pizza" in url):
            return "pizza_pasta_p1"
        elif "page=2" in url:
            return "pizza_pasta_p2"
        elif "page=3" in url:
            return "pizza_pasta_p3"
        elif "frozen-pizza" in url:
            return "frozen_pizza"
    elif "back-to-school" in url.lower():
        if "kidsheadphones" in url:
            if "page=2" in url:
                return "bts_tech_headphones_p2"
            else:
                return "bts_tech_headphones_p1"
        elif "wearables" in url:
            return "bts_tech_wearables"
    elif "deals/school" in url.lower():
        if "page=1" in url or ("page=" not in url and "deals/school" in url):
            return "school_deals_p1"
        elif "page=2" in url:
            return "school_deals_p2"
        elif "page=3" in url:
            return "school_deals_p3"
    elif "infant-car-seats" in url.lower():
        return "infant_car_seats"
    else:
        return "unknown_category"

async def fetch_html(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    """Async HTTP request ile HTML al"""
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"❌ HTTP Error {response.status} for {url}")
                return None
    except Exception as e:
        print(f"❌ Request error for {url}: {e}")
        return None

async def process_single_url_async(session: aiohttp.ClientSession, url: str, index: int, output_dir: str) -> tuple:
    """Tek URL'yi async olarak işle"""
    print(f"\n{'='*60}")
    print(f"🌐 Async İşleniyor ({index+1}): {url}")
    print(f"{'='*60}")
    
    category = extract_category_name(url)
    
    html_content = await fetch_html(session, url)
    
    if not html_content:
        return False, [], []
    
    print(f"📏 HTML boyutu: {len(html_content)} karakter")
    
    # __NEXT_DATA__ scriptini bul
    next_data_pattern = r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>'
    next_data_match = re.search(next_data_pattern, html_content, re.DOTALL)
    
    if next_data_match:
        try:
            next_data_json = next_data_match.group(1)
            next_data = json.loads(next_data_json)
            
            print("✅ __NEXT_DATA__ bulundu ve parse edildi!")
            
            # Kategori klasörü oluştur
            category_dir = os.path.join(output_dir, category)
            if not os.path.exists(category_dir):
                os.makedirs(category_dir)
            
            # JSON'u kategori klasörüne kaydet
            next_data_file = os.path.join(category_dir, f'{category}_next_data.json')
            with open(next_data_file, 'w', encoding='utf-8') as f:
                json.dump(next_data, f, indent=2, ensure_ascii=False)
            print(f"💾 __NEXT_DATA__ {next_data_file}'a kaydedildi")
            
            # Ürünleri ve canonical URL'leri bul
            products = []
            product_ids = []
            canonical_urls = []
            
            def find_products_recursive(obj, path=""):
                """Recursive olarak ürünleri ve canonicalUrl'leri bul"""
                if isinstance(obj, dict):
                    if 'usItemId' in obj:
                        product_id = obj.get('usItemId')
                        name = obj.get('name', 'İsimsiz')
                        price_info = obj.get('priceInfo', {})
                        current_price = price_info.get('currentPrice', {}).get('priceString', 'Fiyat yok')
                        brand = obj.get('brand', 'Marka yok')
                        canonical_url = obj.get('canonicalUrl', '')
                        
                        product = {
                            'id': product_id,
                            'name': name,
                            'price': current_price,
                            'brand': brand,
                            'canonical_url': canonical_url,
                            'category': category,
                            'path': path
                        }
                        products.append(product)
                        product_ids.append(str(product_id))
                        
                        if canonical_url:
                            full_url = f"https://www.walmart.com{canonical_url}"
                            canonical_urls.append(full_url)
                    
                    for key, value in obj.items():
                        find_products_recursive(value, f"{path}.{key}" if path else key)
                        
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        find_products_recursive(item, f"{path}[{i}]" if path else f"[{i}]")
            
            # Ürünleri bul
            find_products_recursive(next_data)
            
            print(f"🛒 {len(products)} ürün bulundu!")
            print(f"🔗 {len(canonical_urls)} canonical URL bulundu!")
            
            # İlk 5 ürünü göster
            for i, product in enumerate(products[:5], 1):
                print(f"\n{i:2d}. 🛍️ {product['name'][:50]}...")
                print(f"     🆔 ID: {product['id']}")
                print(f"     💰 Fiyat: {product['price']}")
                print(f"     🏷️ Marka: {product['brand']}")
                print(f"     🔗 URL: {product['canonical_url']}")
            
            # Dosyaları kaydet
            if product_ids:
                ids_file = os.path.join(category_dir, f'{category}_product_ids.txt')
                with open(ids_file, 'w', encoding='utf-8') as f:
                    for pid in product_ids:
                        f.write(f"{pid}\n")
                print(f"\n📝 {len(product_ids)} ürün ID'si {ids_file}'e kaydedildi")
            
            if canonical_urls:
                urls_file = os.path.join(category_dir, f'{category}_canonical_urls.txt')
                with open(urls_file, 'w', encoding='utf-8') as f:
                    for url in canonical_urls:
                        f.write(f"{url}\n")
                print(f"🔗 {len(canonical_urls)} canonical URL {urls_file}'e kaydedildi")
            
            products_file = os.path.join(category_dir, f'{category}_products.json')
            with open(products_file, 'w', encoding='utf-8') as f:
                json.dump(products, f, indent=2, ensure_ascii=False)
            print(f"💾 Ürün detayları {products_file}'a kaydedildi")
            
            return True, products, canonical_urls
            
        except json.JSONDecodeError as e:
            print(f"❌ __NEXT_DATA__ JSON parse hatası: {e}")
            return False, [], []
        except Exception as e:
            print(f"❌ __NEXT_DATA__ işleme hatası: {e}")
            return False, [], []
    else:
        print("❌ __NEXT_DATA__ script tag'i bulunamadı")
        return False, [], []

async def extract_product_detail_async(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
    """Async olarak tek ürünün detayını al"""
    async with semaphore:  # Concurrent request sayısını sınırla
        try:
            html_content = await fetch_html(session, url)
            if not html_content:
                return None
            
            # Schema.org data'sını extract et
            schema_pattern = r'<script[^>]*type="application/ld\+json"[^>]*data-seo-id="schema-org-product"[^>]*>(.*?)</script>'
            schema_match = re.search(schema_pattern, html_content, re.DOTALL)
            
            if not schema_match:
                schema_pattern = r'<script[^>]*data-seo-id="schema-org-product"[^>]*>(.*?)</script>'
                schema_match = re.search(schema_pattern, html_content, re.DOTALL)
            
            if schema_match:
                try:
                    schema_json = schema_match.group(1)
                    schema_data = json.loads(schema_json)
                    
                    # Ürün bilgilerini extract et
                    product_info = {
                        'url': url,
                        'id': 'Bilinmiyor',
                        'name': 'Bilinmiyor',
                        'price': 'Fiyat yok',
                        'brand': 'Marka yok',
                        'rating': 'Rating yok',
                        'reviews': 'Review yok',
                        'description': 'Açıklama yok',
                        'availability': 'Bilinmiyor',
                        'image_url': 'Resim yok',
                        'sku': 'SKU yok',
                        'category': 'Kategori yok'
                    }
                    
                    # URL'den ID'yi çıkar
                    url_id_match = re.search(r'/ip/[^/]+/(\\d+)', url)
                    if url_id_match:
                        product_info['id'] = url_id_match.group(1)
                    
                    if schema_data and '@type' in schema_data and schema_data['@type'] == 'Product':
                        product_info['name'] = schema_data.get('name', 'Bilinmiyor')
                        product_info['description'] = schema_data.get('description', 'Açıklama yok')
                        
                        # Marka
                        brand = schema_data.get('brand', {})
                        if isinstance(brand, dict):
                            product_info['brand'] = brand.get('name', 'Marka yok')
                        elif isinstance(brand, str):
                            product_info['brand'] = brand
                        
                        product_info['sku'] = schema_data.get('sku', 'SKU yok')
                        
                        # Kategori
                        category = schema_data.get('category', 'Kategori yok')
                        if isinstance(category, list) and category:
                            product_info['category'] = category[0]
                        elif isinstance(category, str):
                            product_info['category'] = category
                        
                        # Resim URL
                        image = schema_data.get('image', [])
                        if isinstance(image, list) and image:
                            product_info['image_url'] = image[0]
                        elif isinstance(image, str):
                            product_info['image_url'] = image
                        
                        # Fiyat bilgisi
                        offers = schema_data.get('offers', {})
                        if isinstance(offers, dict):
                            price = offers.get('price')
                            currency = offers.get('priceCurrency', 'USD')
                            if price:
                                product_info['price'] = f"${price}" if currency == 'USD' else f"{price} {currency}"
                            
                            # Stok durumu
                            availability = offers.get('availability', '')
                            if 'InStock' in availability:
                                product_info['availability'] = 'IN_STOCK'
                            elif 'OutOfStock' in availability:
                                product_info['availability'] = 'OUT_OF_STOCK'
                            else:
                                product_info['availability'] = 'UNKNOWN'
                        
                        # Rating ve review
                        aggregate_rating = schema_data.get('aggregateRating', {})
                        if isinstance(aggregate_rating, dict):
                            product_info['rating'] = aggregate_rating.get('ratingValue', 'Rating yok')
                            product_info['reviews'] = aggregate_rating.get('reviewCount', 'Review yok')
                    
                    return product_info
                    
                except json.JSONDecodeError:
                    return None
            
            return None
            
        except Exception as e:
            print(f"❌ Error processing {url}: {e}")
            return None

async def process_canonical_urls_async(canonical_urls: List[str], output_dir: str, max_concurrent: int = 10) -> List[Dict]:
    """Async olarak canonical URL'leri işle"""
    print(f"\n{'='*80}")
    print(f"🚀 ASYNC 2. AŞAMA: CANONICAL URL'LERDEN DETAYLI ÜRÜN BİLGİLERİ")
    print(f"{'='*80}")
    
    # Product details klasörü
    details_dir = os.path.join(output_dir, "product_details")
    if not os.path.exists(details_dir):
        os.makedirs(details_dir)
        print(f"📁 {details_dir} klasörü oluşturuldu")
    
    # TÜM URL'leri al (Production Mode)
    urls_to_process = canonical_urls  # [:20] sınırı kaldırıldı
    print(f"🔗 {len(urls_to_process)} canonical URL async olarak işlenecek (max {max_concurrent} concurrent)...")
    
    # Semaphore ile concurrent request sayısını sınırla
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        # Async tasks oluştur
        tasks = [
            extract_product_detail_async(session, url, semaphore) 
            for url in urls_to_process
        ]
        
        # Tüm task'leri paralel çalıştır
        print(f"⚡ {len(tasks)} async task başlatılıyor...")
        start_time = time.time()
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Başarılı sonuçları filtrele
        detail_results = []
        successful_count = 0
        
        for i, result in enumerate(results):
            if isinstance(result, dict) and result is not None:
                detail_results.append(result)
                successful_count += 1
                
                url_id = urls_to_process[i].split('/')[-1].split('?')[0]
                print(f"✅ ({successful_count}/{len(urls_to_process)}) {result['name'][:50]}...")
                print(f"   💰 {result['price']} | 🏷️ {result['brand']} | ⭐ {result['rating']} | 👥 {result['reviews']}")
            elif isinstance(result, Exception):
                print(f"❌ Exception for URL {i+1}: {result}")
            else:
                print(f"❌ Failed for URL {i+1}")
        
        print(f"\n⚡ ASYNC İŞLEM TAMAMLANDI!")
        print(f"⏱️ Toplam süre: {processing_time:.2f} saniye")
        print(f"🚀 Hız: {len(urls_to_process)/processing_time:.1f} ürün/saniye")
        print(f"✅ Başarılı: {successful_count}/{len(urls_to_process)}")
        
        # Sonuçları kaydet
        if detail_results:
            results_file = os.path.join(details_dir, "all_product_details_async.json")
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(detail_results, f, indent=2, ensure_ascii=False)
            print(f"💾 Sonuçlar: {results_file}")
        
        return detail_results

async def main():
    """Ana async function"""
    start_time = time.time()
    
    # Klasör oluştur
    output_dir = "scraped_data_async"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 {output_dir} klasörü oluşturuldu")
    
    print(f"🚀 ASYNC WALMART SCRAPING PIPELINE BAŞLIYOR!")
    print(f"🔗 {len(urls)} URL işlenecek...")
    
    all_products = []
    all_canonical_urls = []
    
    # 1. AŞAMA: __NEXT_DATA__ Extraction (Sequential - büyük HTML responses)
    print(f"\n{'='*80}")
    print(f"🌐 1. AŞAMA: __NEXT_DATA__ EXTRACTION")
    print(f"{'='*80}")
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        for i, url in enumerate(urls):
            success, products, canonical_urls = await process_single_url_async(session, url, i, output_dir)
            
            if success:
                all_products.extend(products)
                all_canonical_urls.extend(canonical_urls)
    
    # Genel dosyaları oluştur
    if all_canonical_urls:
        all_urls_file = os.path.join(output_dir, 'all_canonical_urls.txt')
        with open(all_urls_file, 'w', encoding='utf-8') as f:
            for url in all_canonical_urls:
                f.write(f"{url}\n")
        print(f"🔗 Tüm canonical URL'ler {all_urls_file}'e kaydedildi")
    
    if all_products:
        all_products_file = os.path.join(output_dir, 'all_products.json')
        with open(all_products_file, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, indent=2, ensure_ascii=False)
        print(f"💾 Tüm ürünler {all_products_file}'a kaydedildi")
    
    # 2. AŞAMA: Canonical URL'lerden detay alma (Async - Çok hızlı!)
    if all_canonical_urls:
        detail_results = await process_canonical_urls_async(all_canonical_urls, output_dir, max_concurrent=15)
        
        # Final istatistikler
        total_time = time.time() - start_time
        
        print(f"\n{'='*80}")
        print(f"🎉 ASYNC PIPELINE TAMAMLANDI!")
        print(f"{'='*80}")
        print(f"⏱️ Toplam süre: {total_time:.2f} saniye")
        print(f"🛒 Toplam ürün: {len(all_products)}")
        print(f"🔗 Canonical URL: {len(all_canonical_urls)}")
        print(f"📋 Detaylı işlenen: {len(detail_results)}")
        print(f"🚀 Ortalama hız: {len(detail_results)/total_time:.1f} detay/saniye")
        
        # İstatistikler
        if detail_results:
            valid_prices = [r for r in detail_results if r['price'] != 'Fiyat yok']
            valid_ratings = [r for r in detail_results if r['rating'] != 'Rating yok']
            in_stock = [r for r in detail_results if r['availability'] == 'IN_STOCK']
            
            print(f"\n📈 DETAY İSTATİSTİKLER:")
            print(f"   💰 Fiyatı olan ürünler: {len(valid_prices)}/{len(detail_results)}")
            print(f"   ⭐ Rating'i olan ürünler: {len(valid_ratings)}/{len(detail_results)}")
            print(f"   📦 Stokta olan ürünler: {len(in_stock)}/{len(detail_results)}")
        
        print(f"\n🏁 ASYNC WALMART SCRAPING PIPELINE SONUÇLANDI!")
        print(f"📂 Tüm veriler '{output_dir}' klasöründe!")

if __name__ == "__main__":
    asyncio.run(main())
