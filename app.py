import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.5", layout="wide")
st.title("🛡️ 買點定位系統 (自動修復版)")

# --- 1. 資料庫管理與重置功能 ---
if st.sidebar.button("🗑️ 強制重置資料庫 (遇到錯誤請點我)"):
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        st.sidebar.success("資料庫已清除，請重新同步")
        st.rerun()

def to_num(v):
    try:
        if pd.isna(v) or str(v).strip() in ['--', '', 'None']: return 0.0
        return float(str(v).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def save_db(df):
    """確保資料庫格式完全一致"""
    try:
        df = df.copy()
        for col in df.columns:
            df[col] = df[col].astype(str)
        df.to_parquet(DB_FILE, index=False, engine='pyarrow')
        return True
    except Exception as e:
        st.sidebar.error(f"儲存出錯: {e}")
        return False

def load_db():
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            if not df.empty and '日期' in df.columns:
                return df
        except: pass
    return pd.DataFrame()

# --- 2. 強化版同步邏輯 (增加休眠防止封鎖) ---
db = load_db()
last_date_str = db['日期'].max() if not db.empty else "2026-04-20" # 縮短範圍加速初始同步
start_dt = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
today_dt = datetime.now()

if start_dt <= today_dt:
    with st.status("🚀 數據同步中 (請勿關閉網頁)...", expanded=True) as status:
        curr = start_dt
        while curr <= today_dt:
            d_str = curr.strftime("%Y-%m-%d")
            if curr.weekday() >= 5: # 跳過週末
                curr += timedelta(days=1)
                continue
                
            try:
                time.sleep(3) # 增加到 3 秒，確保證交所不封鎖
                r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=25)
                data = r.json()
                
                if data.get('stat') == 'OK' and data.get('data'):
                    new_df = pd.DataFrame(data['data'], columns=data['fields'])
                    new_df.columns = [c.strip() for c in new_df.columns]
                    new_df['日期'] = d_str
                    
                    # 檢查是否具備關鍵欄位再合併
                    if any('收盤價' in c for c in new_df.columns):
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        save_db(db)
                        st.write(f"✅ {d_str} 同步成功")
                    else:
                        st.write(f"⚠️ {d_str} 數據內容不全，跳過")
                else:
                    st.write(f"🏮 {d_str} 休市或無數據")
            except Exception as e:
                st.write(f"⏳ {d_str} 網路繁忙，暫停同步")
                break 
            curr += timedelta(days=1)
        status.update(label="同步流程結束", state="complete")

# --- 3. 極致防護報表顯示 ---
if not db.empty:
    try:
        latest = db['日期'].max()
        df_now = db[db['日期'] == latest].copy()
        
        # 動態欄位識別
        buy_col = next((c for c in db.columns if '三大法人買賣超股數' in c or '合計' in c), None)
        price_col = next((c for c in db.columns if '收盤價' in c), None)
        
        if buy_col and price_col:
            res = []
            for _, row in df_now.iterrows():
                sid = row['證券代號']
                vol = round(to_num(row[buy_col]) / 1000, 0)
                
                # 計算歷史數據
                hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False).head(5)
                p_vals = [to_num(v) for v in hist[price_col].tolist() if to_num(v) > 0]
                ma5 = round(sum(p_vals)/len(p_vals), 2) if p_vals else 0
                curr_p = to_num(row[price_col])
                diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
                
                res.append({
                    '日期': latest, '股票代號': sid, '股票名稱': row['證券名稱'],
                    '關鍵分點': "三大法人", '買超張數': int(vol), '5日均價': ma5,
                    '目前現價': curr_p, '價差%': f"{diff_pct:.2%}",
                    '連續出現天數': len(hist), '集保人數變動': "-", 
                    '最佳購買日期': latest if abs(diff_pct) <= 0.01 else "觀望",
                    '操盤建議': "多頭趨勢" if curr_p > ma5 else "區間整理"
                })
            
            st.subheader(f"📅 分析報表 - {latest}")
            st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.error("❌ 目前抓到的資料格式不符（缺少收盤價或買賣超），請點擊左側「強制重置資料庫」按鈕後重新同步。")
    except Exception as e:
        st.error(f"報表生成錯誤，請重置資料庫。錯誤代碼: {e}")
else:
    st.warning("⚠️ 資料庫目前為空。請確認網際網路連線，並讓系統自動補齊數據。")
