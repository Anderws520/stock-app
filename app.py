import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.7", layout="wide")
st.title("🛡️ 買點定位系統 (耐力同步版)")

def to_num(v):
    try:
        s = str(v).replace(',', '').replace(' ', '').strip()
        if s in ['--', '', 'None', 'nan']: return 0.0
        return float(s)
    except: return 0.0

def load_db():
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            if not df.empty and '收盤價' in "".join(df.columns):
                return df
            os.remove(DB_FILE) # 損壞就移除
        except:
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
    return pd.DataFrame()

def save_db(df):
    try:
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except: return False

# --- 執行增量同步 ---
db = load_db()
# 鎖定近期範圍，避免過長請求導致封鎖
last_date_str = db['日期'].max() if not db.empty else "2026-04-20"
curr_dt = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
today_dt = datetime.now()

if curr_dt <= today_dt:
    with st.status("🚀 正在執行耐力同步 (失敗將自動重試)...", expanded=True) as status:
        while curr_dt <= today_dt:
            d_str = curr_dt.strftime("%Y-%m-%d")
            if curr_dt.weekday() >= 5: # 跳過週末
                curr_dt += timedelta(days=1)
                continue
            
            success = False
            retry_count = 0
            while not success and retry_count < 3: # 遇到繁忙自動重試 3 次
                try:
                    time.sleep(4) # 延長等待時間，徹底防封鎖
                    r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=30)
                    data = r.json()
                    
                    if data.get('stat') == 'OK' and data.get('data'):
                        new_df = pd.DataFrame(data['data'], columns=data['fields'])
                        new_df.columns = [c.strip() for c in new_df.columns]
                        # 核心欄位存在判定
                        if any('收盤價' in c for c in new_df.columns):
                            new_df['日期'] = d_str
                            db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                            save_db(db)
                            st.write(f"✅ {d_str} 同步完成")
                            success = True
                    else:
                        st.write(f"🏮 {d_str} 證交所無開盤數據")
                        success = True # 無開盤也算完成
                except:
                    retry_count += 1
                    st.write(f"⏳ {d_str} 連線繁忙，第 {retry_count} 次重新連線中...")
                    time.sleep(10) # 繁忙時停頓更久
            
            if not success:
                st.error(f"❌ {d_str} 無法取得數據，請稍後重整網頁")
                break
            curr_dt += timedelta(days=1)
        status.update(label="數據同步檢查結束", state="complete")

# --- 報表顯示 ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    
    # 動態對齊欄位
    buy_col = next((c for c in db.columns if '買賣超股數' in c or '合計' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    if buy_col and price_col:
        res = []
        for _, row in df_now.iterrows():
            sid = row['證券代號']
            shares = to_num(row[buy_col])
            if shares <= 0: continue
            
            # 歷史均價計算備援
            hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False)
            p_list = [to_num(p) for p in hist[price_col].head(5).tolist() if to_num(p) > 0]
            ma5 = round(sum(p_list)/len(p_list), 2) if p_list else to_num(row[price_col])
            
            curr_p = to_num(row[price_col])
            diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
            
            res.append({
                '日期': latest, '股票代號': sid, '股票名稱': row['證券名稱'],
                '買超張數': int(round(shares/1000, 0)), '5日均價': ma5, '目前現價': curr_p,
                '價差%': f"{diff_pct:.2%}", '歷史天數': len(hist),
                '操盤建議': "雙強初現" if shares > 500000 and len(hist) <= 2 else "趨勢延續"
            })
        
        st.subheader(f"📊 分析報表：{latest}")
        st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 抓取的資料格式仍有問題，請點擊重試。")
else:
    st.info("💡 系統正嘗試第一次抓取資料，請耐心等待「同步完成」字樣出現。")
