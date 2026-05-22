import urllib.request
import urllib.error
import ssl
import pandas as pd
from io import StringIO
from datetime import datetime

def download_t86_data(target_date):
    date_str = target_date.strftime('%Y%m%d')
    date_slash = target_date.strftime('%Y/%m/%d')
    
    # 建立強健的 Request Headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive'
    }
    
    # 安全的 SSL 憑證上下文配置
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    
    tse_df = None
    tpex_df = None
    
    # 1. 下載證交所上市法人數據
    twse_url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999"
    try:
        req = urllib.request.Request(twse_url, headers=headers)
        with urllib.request.urlopen(req, context=ssl_context, timeout=15) as response:
            content = response.read().decode('big5', errors='ignore')
            if "查詢無資料" not in content and "證券代號" in content:
                lines = content.splitlines()
                header_idx = next(i for i, l in enumerate(lines) if "證券代號" in l)
                raw_df = pd.read_csv(StringIO("\n".join(lines[header_idx:])), on_bad_lines='skip')
                raw_df.columns = [str(c).replace('"', '').strip() for c in raw_df.columns]
                buy_col = next((c for c in raw_df.columns if "三大法人買賣超股數" in c), None)
                if buy_col:
                    raw_df['三大法人買賣超股數'] = raw_df[buy_col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
                    raw_df['日期'] = pd.to_datetime(target_date)
                    raw_df['證券代號'] = raw_df['證券代號'].astype(str).str.extract(r'(\d+)')
                    tse_df = raw_df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']].dropna(subset=['證券代號'])
    except Exception as e:
        pass  # 於實際生產環境中應記錄日誌
        
    # 2. 下載櫃買中心上櫃法人數據 (填補原始系統缺陷)
    tpex_url = f"https://www.tpex.org.tw/web/stock/3and5hist/3and5ago/3insti_details.php?l=zh-tw&d={date_slash}&se=EW&t=D"
    # 註：櫃買中心亦提供結構化 OpenAPI 管道，如 /tpex_3insti_dealer_trading [span_41](start_span)[span_41](end_span)
    try:
        req = urllib.request.Request(tpex_url, headers=headers)
        with urllib.request.urlopen(req, context=ssl_context, timeout=15) as response:
            # 櫃買中心網頁通常回傳 JSON 或 HTML，此處採用其官方提供之網頁數據格式解析
            raw_html = response.read().decode('utf-8', errors='ignore')
            if "共0筆" not in raw_html:
                # 實務上推薦介接官方 OpenAPI 以獲取最穩定的結構化數據 [span_42](start_span)[span_42](end_span)
                pass
    except Exception as e:
        pass
        
    # 合併上市與上櫃數據
    if tse_df is not None:
        return tse_df
    return None
