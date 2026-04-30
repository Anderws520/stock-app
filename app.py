import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import random

st.set_page_config(page_title="專業操盤系統-終極穩定版", layout="wide")
st.title("🛡️ 買點定位系統 (官方數據校準版)")

# --- 側邊欄設定 ---
st.sidebar.header("📅 交易時間軸")
start_date = st.sidebar.date_input("分析起始日期", datetime(2026, 4, 20))
end_date = st.sidebar.date_input("分析結束日期", datetime(2026, 4, 30))

def fetch_with_retry(url, name="數據"):
    """
    專業級抓取：帶有隨機延遲與偽裝標頭，防止被封鎖
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/zh/page/trading/fund/T86.html'
    }
    try:
        # 增加隨機等待，模擬真人行為
        time.sleep(random.uniform(2.0, 4.0)) 
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        st.warning(f"抓取 {name} 時發生微小異常，嘗試繞過中...")
    return None

if st.button("🚀 啟動高勝率大數據掃描"):
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    final_data_list = []
    
    progress_text = st.empty()
    p_bar = st.progress(0)
    
    for i, d in enumerate(date_list):
        progress_text.text(f"正在分析 {d} 的法人籌碼...")
        
        # 1. 抓取三大法人資料
        chip_url = f"https://www.twse.com.tw/fund/T86?response=json&date={d}&selectType=ALL"
        chip_json = fetch_with_retry(chip_url, "籌碼")
        
        # 2. 抓取收盤價資料
        price_url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={d}&type=ALLBUT0999"
        price_json = fetch_with_retry(price_url, "價格")
        
        if chip_json and chip_json.get('stat') == 'OK' and price_json and price_json.get('stat') == 'OK':
            # 解析籌碼
            df_chips = pd.DataFrame(chip_json['data'], columns=chip_json['fields'])
            # 解析價格 (MI_INDEX 的資料通常在 data9)
            p_key = 'data9' if 'data9' in price_json else ('data8' if 'data8' in price_json else None)
            if p_key:
                df_price = pd.DataFrame(price_json[p_key], columns=price_json['fields9' if 'data9' in price_json else 'fields8'])
                # 只取代號與收盤價
                df_price = df_price[['證券代號', '收盤價']]
                # 合併
                merged = pd.merge(df_chips, df_price, on='證券代號', how='left')
                merged['日期'] = d
                final_data_list.append(merged)
        
        p_bar.progress((i + 1) / len(date_list))

    if final_data_list:
        full_df = pd.concat(final_data_list)
        
        # --- 核心邏輯處理 ---
        results = []
        for sid, group in full_df.groupby('證券代號'):
            group = group.sort_values('日期')
            
            # 清理數值
            def clean(v): return pd.to_numeric(str(v).replace(',',''), errors='coerce') or 0
            
            prices = [clean(p) for p in group['收盤價']]
            curr_p = prices[-1]
            ma5 = sum(prices[-5:]) / len(prices[-5:]) if prices else 0
            
            last_row = group.iloc[-1]
            # 依據官方標籤精確抓取
            f_buy = clean(last_row['外陸資買賣超股數(不含外資自營商)']) / 1000
            i_buy = clean(last_row['投信買賣超股數']) / 1000
            total_buy = round(f_buy + i_buy, 0)
            
            if total_buy > 300: # 門檻過濾
                advice, rank = ("💎 雙強初現(首選)", 1) if len(group) <= 2 else (("🔥 趨勢續強", 2) if len(group) >= 3 else ("✅ 趨勢跟蹤", 3))
                results.append({
                    '日期': last_row['日期'],
                    '股票代號': sid,
                    '股票名稱': last_row['證券名稱'],
                    '買超張數': int(total_buy),
                    '目前現價': round(curr_p, 2),
                    '5日均價': round(ma5, 2),
                    '價差%': f"{((curr_p-ma5)/ma5):.2%}" if ma5 != 0 else "0.00%",
                    '連續天數': len(group),
                    '操盤建議': advice,
                    'rank': rank
                })

        final_df = pd.DataFrame(results).sort_values(['rank', '買超張數'], ascending=[True, False]).head(20)
        st.success("✅ 數據校準成功！")
        st.dataframe(final_df.drop(columns=['rank']), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 期間內無任何有效交易數據，或 API 連線受阻。")
        st.info("💡 如果頻繁看到此訊息，建議將分析日期區間縮小（例如只看 3 天），以降低證交所的連線壓力。")
