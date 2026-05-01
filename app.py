import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.4", layout="wide")
st.title("🛡️ 買點定位系統 (穩定更新版)")

def to_num(v):
    try:
        if pd.isna(v) or v == '--': return 0.0
        return float(str(v).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def save_db(df):
    """確保資料庫以字串儲存，解決 ArrowTypeError"""
    try:
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except: return False

def load_db():
    if os.path.exists(DB_FILE):
        try: return pd.read_parquet(DB_FILE)
        except: return pd.DataFrame()
    return pd.DataFrame()

# --- 改進版同步邏輯 ---
db = load_db()
last_date = db['日期'].max() if not db.empty else "2026-01-01"
start_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
today_dt = datetime.now()

if start_dt <= today_dt:
    with st.status("🚀 正在補齊歷史數據 (每日間隔 2 秒以防封鎖)...", expanded=True) as status:
        curr = start_dt
        while curr <= today_dt:
            d_str = curr.strftime("%Y-%m-%d")
            # 僅過濾週末，國定假日由 API 回傳值判定
            if curr.weekday() >= 5:
                curr += timedelta(days=1)
                continue
                
            try:
                # 增加延遲，避免被證交所阻擋導致「請求跳過」
                time.sleep(2) 
                r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=20)
                data = r.json()
                
                if data.get('stat') == 'OK' and data.get('data'):
                    new_df = pd.DataFrame(data['data'], columns=data['fields'])
                    new_df.columns = [c.strip() for c in new_df.columns]
                    new_df['日期'] = d_str
                    # 確保必要欄位存在才合併
                    if any('收盤價' in c for c in new_df.columns):
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        save_db(db)
                        st.write(f"✅ {d_str} 同步成功")
                    else:
                        st.write(f"⚠️ {d_str} 資料格式異常")
                else:
                    st.write(f"🏮 {d_str} 證交所無開盤數據")
            except Exception as e:
                st.write(f"⏳ {d_str} 請求頻繁，暫時跳過，稍後自動重試")
                break # 碰到封鎖先停止，不盲目跑完
            curr += timedelta(days=1)
        status.update(label="同步階段完成", state="complete")

# --- 強化版報表 (防 KeyError) ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    
    # 欄位安全識別
    buy_col = next((c for c in db.columns if '三大法人買賣超股數' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    if buy_col and price_col:
        res = []
        for _, row in df_now.iterrows():
            sid = row['證券代號']
            vol = round(to_num(row[buy_col]) / 1000, 0)
            
            # 取得歷史資料計算均價
            hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False).head(5)
            p_vals = [to_num(v) for v in hist[price_col].tolist()]
            ma5 = round(sum(p_vals)/len(p_vals), 2) if p_vals else 0
            
            curr_p = to_num(row[price_col])
            diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
            # 算出法人連續買超天數
            days = len(db[(db['證券代號'] == sid) & (db[buy_col].apply(to_num) > 0)])

            res.append({
                '日期': latest, '股票代號': sid, '股票名稱': row['證券名稱'],
                '關鍵分點': "三大法人", '買超張數': int(vol), '5日均價': ma5,
                '目前現價': curr_p, '價差%': f"{diff_pct:.2%}", '連續出現天數': days,
                '集保人數變動': "無數據", '最佳購買日期': latest if abs(diff_pct) <= 0.01 else "觀望",
                '操盤建議': "多頭" if curr_p > ma5 else "整理"
            })

        st.subheader(f"📊 最新分析日期：{latest}")
        st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 資料庫雖然存在，但內容缺失關鍵欄位，請稍候讓系統補齊數據。")
else:
    st.warning("📭 資料庫為空，請點擊上方重新同步數據。")
