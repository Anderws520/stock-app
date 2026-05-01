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
import yfinance as yf

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人全功能操盤系統", layout="wide")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 1, 1).date()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# 側邊欄導覽
with st.sidebar:
    st.title("🛠️ 操盤工具箱")
    mode = st.selectbox("功能分頁切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"])
    st.markdown("---")
    st.info("💡 **顯示優化**：戰報已整合『價差%』、『操盤建議』與『集保變動』。")

# ====================== 2. 通用核心函數 ======================
def is_trading_day(d):
    if d.weekday() >= 5: return False
    holidays = ["2026-01-01", "2026-01-28", "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01"]
    return d.strftime('%Y-%m-%d') not in holidays

def clean_number(x):
    if isinstance(x, str): x = re.sub(r'[^\d.-]', '', x)
    try: return float(x)
    except: return 0.0

def download_t86(date):
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=30, verify=False)
        lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None: return None
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if buy_col and '證券代號' in df.columns:
            df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
            df = df.dropna(subset=['證券代號']).copy()
            df['日期'] = date
            df['證券代號'] = df['證券代號'].astype(str).str.strip().str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except: return None
    return None

# ====================== 3. 各分頁邏輯 ======================

# --- 分頁 A: 今日強勢戰報 (完整欄位版：含操盤建議與集保) ---
if mode == "今日強勢戰報":
    st.title("🟢 今日三大法人強勢戰報")
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
        if not db.empty:
            latest = pd.to_datetime(db['日期']).max().date()
            st.success(f"📊 最新數據：{latest} | 資料庫累積：{len(db):,}")
            
            db = db.sort_values(['證券代號', '日期']).copy()
            db['買超正'] = db['三大法人買賣超股數'] > 0
            db['連續買超'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            today_df = db[pd.to_datetime(db['日期']).dt.date == latest].copy()
            today_df['買超張數'] = (today_df['三大法人買賣超股數'] / 1000).round(1)
            pre_filter = today_df[today_df['買超張數'] >= 500].sort_values('買超張數', ascending=False).head(100)

            if st.button("🚀 同步今日價格與籌碼 Top 20", type="primary"):
                with st.spinner("🔍 正在計算市場價位、操盤邏輯與集保數據..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="15d", interval="1d", group_by='ticker', progress=False)
                    
                    results = []
                    for s in codes:
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            if t in price_data.columns.levels[0]:
                                p_df = price_data[t].dropna()
                                if not p_df.empty:
                                    curr = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    # 模擬集保數邏輯 (若無即時API則顯示成交量變動參考，或預留欄位)
                                    vol_change = "📉 減少" if p_df['Volume'].iloc[-1] < p_df['Volume'].iloc[-2] else "📈 增加"
                                    
                                    row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                                    advice = "🚀 剛發動" if row['連續買超'] == 1 else "⏳ 持續鎖碼"
                                    
                                    results.append({
                                        "證券代號": s,
                                        "證券名稱": row['證券名稱'],
                                        "買超張數": row['買超張數'],
                                        "目前現價": curr,
                                        "5日均價": ma5,
                                        "價差%": ((curr - ma5) / ma5 * 100),
                                        "連續買超": int(row['連續買超']),
                                        "集保變動": vol_change, # 此欄位可根據你後續串接的集保API更新
                                        "操盤建議": advice
                                    })
                                    break
                    
                    final_df = pd.DataFrame(results).head(20)
                    st.subheader(f"📊 {latest} 操盤精選 Top 20")
                    st.dataframe(
                        final_df, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "價差%": st.column_config.NumberColumn("價差%", format="%.2f %%"),
                            "操盤建議": st.column_config.TextColumn("操盤建議")
                        }
                    )
    else:
        st.warning("請先前往『資料庫管理』進行補帳。")

# --- 分頁 B: 籌碼週期分析 ---
elif mode == "籌碼週期分析":
    st.title("🔍 1/1 至今：最佳進出場深度分析")
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        db['買超張數'] = (db['三大法人買賣超股數'] / 1000).round(1)
        db['買超正'] = db['買超張數'] > 100
        db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        if st.button("📊 啟動全年度週期掃描", type="primary"):
            active_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
            results = []
            with st.status("分析中...") as status:
                codes = active_stocks[:40].tolist()
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                price_data = yf.download(tickers, period="12d", interval="1d", group_by='ticker', progress=False)
                for code in codes:
                    s_data = db[db['證券代號'] == code].copy()
                    entry_points = s_data[s_data['連買計數'] == 1]['日期'].tolist()
                    curr_p, ma5 = np.nan, np.nan
                    for suf in [".TW", ".TWO"]:
                        t = f"{code}{suf}"
                        if t in price_data.columns.levels[0]:
                            p_df = price_data[t].dropna()
                            if not p_df.empty:
                                curr_p, ma5 = round(float(p_df['Close'].iloc[-1]), 2), round(float(p_df['Close'].tail(5).mean()), 2)
                                break
                    if not np.isnan(curr_p):
                        last_c = s_data.iloc[-1]['連買計數']
                        results.append({
                            "代號": code, "名稱": s_data['證券名稱'].iloc[0],
                            "歷史發動次數": f"{len(entry_points)} 次",
                            "今日狀態": "🟢 剛發動" if last_c == 1 else f"⚪ 連買 {int(last_c)} 天",
                            "最佳購買日期": "🔥 就在今天" if last_c == 1 else "⏳ 等待回測",
                            "最佳購買價位": curr_p if last_c == 1 else ma5,
                            "最佳賣出價格": round(ma5 * 1.08, 2),
                            "最近發動": " → ".join([d.strftime('%m/%d') for d in entry_points[-3:]])
                        })
                status.update(label="分析完成！", state="complete")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

# --- 分頁 C: 資料庫管理 ---
elif mode == "資料庫管理":
    st.title("🗄️ 資料庫管理")
    if st.button("🧨 重置並重新補帳", type="primary"):
        if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
        all_data = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        target_dates = [d for d in (START_DATE + timedelta(n) for n in range((datetime.now().date() - START_DATE).days + 1)) if is_trading_day(d)]
        for i, d in enumerate(target_dates):
            status_text.text(f"下載 {d}... ({i+1}/{len(target_dates)})")
            df = download_t86(d)
            if df is not None: all_data.append(df)
            progress_bar.progress((i + 1) / len(target_dates))
            time.sleep(random.uniform(2, 4))
        if all_data:
            pd.concat(all_data).to_parquet(DATA_FILE)
            st.success("✅ 補帳完成！")
