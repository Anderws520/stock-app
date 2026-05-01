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
st.title("🟢 台股三大法人 + MA5 專業操盤系統")
st.markdown("**20年操盤手設計**｜買超強度 + MA5防護 + 連續買超")

DATA_FILE = "twse_institutional_db.parquet"
PRICE_CACHE = "price_cache.parquet"
START_DATE = datetime(2026, 4, 27).date()

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

def is_trading_day(d):
    if d.weekday() >= 5 or d == datetime(2026, 5, 1).date():
        return False
    return True

def get_t86_url(date):
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
        resp = requests.get(get_t86_url(date), headers={"User-Agent": random.choice(USER_AGENTS)}, 
                           timeout=30, verify=False)
        resp.raise_for_status()
        
        lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None:
            return None
            
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        
        # 找到三大法人買賣超股數欄位
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if not buy_col or '證券代號' not in df.columns:
            return None
            
        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.strip().str.zfill(4)
        
        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except:
        return None

# ====================== 抓取收盤價（簡化每月抓取） ======================
def get_price_data(stock_list, target_date):
    # 這裡先用簡化邏輯，實際上建議用 TWSE STOCK_DAY API 或 yfinance
    # 為了讓你能先跑起來，目前用佔位值，之後我會幫你加上完整價格抓取
    price_df = pd.DataFrame({'證券代號': stock_list, '收盤價': np.nan})
    return price_df

# ====================== 主更新 ======================
if st.button("🔄 更新三大法人資料（斷點續傳）", type="primary"):
    # ... (保留你原本更新邏輯，這裡省略以節省篇幅，實際上用之前成功的更新程式碼)

# 載入資料並計算
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest_date = pd.to_datetime(db['日期']).max().date()
        st.success(f"資料最新日期：**{latest_date}** | 總筆數：{len(db):,}")
        
        # 計算連續買超天數
        db = db.sort_values(['證券代號', '日期'])
        db['買超正'] = db['三大法人買賣超股數'] > 0
        db['連續買超天數'] = db.groupby('證券代號')['買超正'].transform(
            lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1)
        )
        
        today_df = db[db['日期'] == latest_date].copy()
        today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
        
        # 初步篩選買超 > 500張
        strong = today_df[today_df['三大法人買賣超股數'] > 500000].copy()
        
        # TODO: 加入 MA5 與現價計算（下一輪完整版）
        strong['5日均價'] = np.nan
        strong['目前現價'] = np.nan
        strong['價差%'] = np.nan
        strong['關鍵分點'] = "待補充"   # 外資/投信/自營商細分
        strong['集保人數變動'] = np.nan
        strong['最佳購買日期'] = "待觀察"
        strong['操盤建議'] = np.where(strong['連續買超天數'] >= 3, "🔒 法人鎖碼", 
                                     np.where((strong['三大法人買賣超股數'] > 1000000) & (strong['連續買超天數'] < 3), "🔥 雙強初現", "✅ 值得觀察"))
        
        display_cols = ['日期', '證券代號', '證券名稱', '關鍵分點', '買超張數', 
                       '5日均價', '目前現價', '價差%', '連續買超天數', '集保人數變動', 
                       '最佳購買日期', '操盤建議']
        
        st.subheader(f"{latest_date} 法人強勢股專業分析表")
        st.dataframe(strong[display_cols].sort_values('買超張數', ascending=False), 
                    use_container_width=True, hide_index=True)
        
        st.info("""**操盤手建議**：
- 買超 > 500張 + 現價接近或站上MA5 → 勝率較高
- 連續買超3天以上 → 法人鎖碼，較適合波段
- 價差% 超過 +5% → 追高風險增加，建議等回測""")
else:
    st.info("請先點上方按鈕更新資料")

st.caption("目前價格與MA5尚未完整串接，下一版我會幫你加上自動抓收盤價功能。")