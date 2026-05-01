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
st.markdown("**20年實戰操盤手設計**｜完整版：買超 + 關鍵分點 + MA5防護")

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
        return 0.0

# ====================== 下載三大法人 ======================
def download_t86(date):
    if not is_trading_day(date):
        return None
    try:
        resp = requests.get(get_t86_url(date), 
                           headers={"User-Agent": random.choice(USER_AGENTS)}, 
                           timeout=30, verify=False)
        resp.raise_for_status()
        
        lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None:
            return None
            
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        
        # 三大法人買賣超
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if not buy_col:
            return None
            
        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        
        # 關鍵分點：外資、投信、自營商買超
        foreign_col = next((col for col in df.columns if "外陸資買賣超" in col or "外資買賣超" in col), None)
        trust_col = next((col for col in df.columns if "投信買賣超" in col), None)
        dealer_col = next((col for col in df.columns if "自營商買賣超" in col), None)
        
        df['外資買超'] = df[foreign_col].apply(clean_number) if foreign_col else 0
        df['投信買超'] = df[trust_col].apply(clean_number) if trust_col else 0
        df['自營商買超'] = df[dealer_col].apply(clean_number) if dealer_col else 0
        
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.strip().str.zfill(4)
        
        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數', 
                   '外資買超', '投信買超', '自營商買超']]
    except:
        return None

# ====================== 更新資料 ======================
if st.button("🔄 更新三大法人資料（斷點續傳）", type="primary"):
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
    else:
        db = pd.DataFrame()
    
    last_date = START_DATE - timedelta(days=1) if db