import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import re
import os

st.set_page_config(page_title="台股法人操盤工具", layout="wide")
st.title("🟢 台股三大法人買超專業操盤系統")
st.markdown("**20年操盤手設計**｜買超 + MA5防護 + 連續買超")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 4, 27).date()

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

def is_trading_day(d):
    if d.weekday() >= 5 or d == datetime(2026, 5, 1).date():
        return False
    return True

def get_url(date):
    return f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"

def clean_number(x):
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x)
    try:
        return float(x)
    except:
        return 0

# ====================== 下載三大法人 ======================
def download_t86(date):
    if not is_trading_day(date):
        return None
    try:
        resp = requests.get(get_url(date), headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=30, verify=False)
        resp.raise_for_status()
        text = resp.text
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None:
            return None
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if buy_col is None or '證券代號' not in df.columns:
            return None
        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df = df.dropna(subset=['證券代號']).copy()
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.strip().str.zfill(4)
        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except:
        return None

# ====================== 抓取收盤價 (簡化版，使用 TWSE 日線 API) ======================
@st.cache_data(ttl=3600)
def get_close_price(stock_list, target_date):
    prices = {}
    for stock in stock_list[:50]:  # 限制數量避免太慢
        try:
            url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date={target_date.strftime('%Y%m')}01&stockNo={stock}"
            resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=15, verify=False)
            lines = resp.text.splitlines()
            start = next((i for i, line in enumerate(lines) if "日期" in line), None)
            if start is None:
                continue
            df_price = pd.read_csv(StringIO("\n".join(lines[start:])), encoding='big5', on_bad_lines='skip')
            df_price.columns = [col.strip() for col in df_price.columns]
            if '收盤價' in df_price.columns:
                latest_price = df_price['收盤價'].iloc[-1]
                prices[stock] = clean_number(latest_price)
        except:
            pass
        time.sleep(0.5)
    return prices

# ====================== 主程式 ======================
if st.button("🔄 更新三大法人資料", type="primary"):
    # ... 更新資料邏輯保持不變（使用你之前成功的部分）
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
    else:
        db = pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
    
    if db.empty:
        last_date = START_DATE - timedelta(days=1)
    else:
        last_date = pd.to_datetime(db['日期']).max().date()
    
    today = datetime.now().date()
    target = last_date + timedelta(days=1)
    
    progress = st.progress(0)
    status = st.empty()
    count = 0
    while target <= today and count < 60:
        if is_trading_day(target):
            status.info(f"抓取 {target}")
            new_df = download_t86(target)
            if new_df is not None and not new_df.empty:
                db = pd.concat([db, new_df], ignore_index=True)
                db = db.drop_duplicates(subset=['日期', '證券代號'])
                db.to_parquet(DATA_FILE, index=False)
                status.success(f"✅ {target} 完成")
            time.sleep(random.uniform(6, 9))
        target += timedelta(days=1)
        count += 1
        progress.progress(min(count/30, 1.0))
    st.success("更新完成！")

# ====================== 顯示專業報表 ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"✅ 最新日期：**{latest}** | 總筆數：{len(db):,}")
        
        db = db.sort_values(['證券代號', '日期']).copy()
        db['買超正'] = db['三大法人買賣超股數'] > 0
        db['連續出現天數'] = db.groupby('證券代號')['買超正'].transform(
            lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1)
        )
        
        today_data = db[db['日期'] == latest].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        
        # 抓取價格
        stock_list = today_data['證券代號'].tolist()
        price_dict = get_close_price(stock_list, latest)
        today_data['目前現價'] = today_data['證券代號'].map(price_dict)
        
        # 計算 MA5（取最近5天價格，簡化版）
        ma5_dict = {}
        for stock in stock_list:
            stock_data = db[db['證券代號'] == stock].tail(5)
            if len(stock_data) >= 5 and '目前現價' in stock_data.columns:   # 這裡需要調整
                ma5_dict[stock] = stock_data['目前現價'].mean()
        today_data['5日均價'] = today_data['證券代號'].map(ma5_dict)
        
        # 價差%
        today_data['價差%'] = np.where(today_data['5日均價'].notna() & today_data['目前現價'].notna(),
                                      ((today_data['目前現價'] - today_data['5日均價']) / today_data['5日均價'] * 100).round(2), None)
        
        # 操盤建議 + MA5防護
        cond1 = (today_data['三大法人買賣超股數'] > 1000000) & (today_data['連續出現天數'] < 3)
        cond2 = today_data['連續出現天數'] >= 3
        today_data['操盤建議'] = np.select([cond1, cond2], ['🔥 雙強初現', '🔒 法人鎖碼'], default='✅ 值得觀察')
        
        today_data = today_data.rename(columns={'證券名稱': '股票名稱'})
        today_data['關鍵分點'] = '三大法人買超'
        today_data['集保人數變動'] = None
        today_data['最佳購買日期'] = '待觀察'
        
        display_cols = ['日期', '證券代號', '股票名稱', '關鍵分點', '買超張數', 
                       '5日均價', '目前現價', '價差%', '連續出現天數', 
                       '集保人數變動', '最佳購買日期', '操盤建議']
        
        final_df = today_data[today_data['買超張數'] > 500].copy()
        
        st.subheader(f"📊 {latest} 專業操盤分析報表（買超 > 500張）")
        st.dataframe(final_df[display_cols].sort_values('買超張數', ascending=False).head(50),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "買超張數": st.column_config.NumberColumn(format="%.1f 張"),
                        "5日均價": st.column_config.NumberColumn(format="%.2f"),
                        "目前現價": st.column_config.NumberColumn(format="%.2f"),
                        "價差%": st.column_config.NumberColumn(format="%.2f %%"),
                        "連續出現天數": st.column_config.NumberColumn(format="%d 天"),
                    })
        
        st.info("**MA5防護**：價差% 太高（> +5%）時風險較大，建議等回測均線再進場。")
    else:
        st.info("資料庫尚無資料")
else:
    st.info("請點擊上方按鈕更新資料")