import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import os
import yfinance as yf

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d.strftime('%Y-%m-%d') == "2026-05-01": return False  # 勞動節
    return True

def parse_t86_text(text_data, target_date):
    """核心邏輯：負責將證交所原始CSV文字解析為DataFrame"""
    try:
        if "查詢無資料" in text_data: 
            return "NODATA"
        lines = text_data.splitlines()
        header_idx = -1
        for i, l in enumerate(lines):
            if "證券代號" in l:
                header_idx = i
                break
        if header_idx == -1: 
            return None
            
        df = pd.read_csv(StringIO("\n".join(lines[header_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(c).replace('"', '').strip() for c in df.columns]
        buy_col = next((c for c in df.columns if "三大法人買賣超股數" in c), None)
        
        if buy_col:
            df['三大法人買賣超股數'] = df[buy_col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
            df['日期'] = pd.to_datetime(target_date)
            df['證券代號'] = df['證券代號'].astype(str).str.extract(r'(\d+)')[0]
            return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']].dropna(subset=['證券代號'])
    except:
        pass
    return None

def download_t86_csv(target_date):
    date_str = target_date.strftime('%Y%m%d')
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999"
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=20, verify=False)
        return parse_t86_text(resp.text, target_date)
    except:
        return None

# ====================== 側邊欄：更新與管理 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    last_date = None
    if os.path.exists(DATA_FILE):
        try:
            db_info = pd.read_parquet(DATA_FILE)
            if not db_info.empty:
                last_date = pd.to_datetime(db_info['日期']).max().date()
                st.success(f"📁 目前資料庫至：{last_date}")
        except:
            st.error("📁 Parquet 資料庫檔案毀損，若持續無法更新請刪除舊檔重跑。")

    st.subheader("🔄 數據更新軌道")
    update_method = st.radio("更新方式", ["1. 官方自動續傳", "2. 遭阻擋時手動貼上"], index=0)

    if update_method == "1. 官方自動續傳":
        if st.button("運行自動續傳", type="primary", use_container_width=True):
            with st.container():
                db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
                start_point = (last_date + timedelta(days=1)) if last_date else datetime(2026, 4, 27).date()
                today = datetime.now().date()
                curr = start_point
                
                status_text = st.empty()
                p_bar = st.progress(0)
                total_days = (today - start_point).days + 1
                
                while curr <= today:
                    status_text.text(f"⏳ 正在下載原始資料: {curr}")
                    if is_trading_day(curr):
                        day_df = download_t86_csv(curr)
                        
                        if isinstance(day_df, pd.DataFrame) and not day_df.empty:
                            db = pd.concat([db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                            db.to_parquet(DATA_FILE, index=False)
                            st.toast(f"✅ {curr} 原始資料下載成功！")
                            time.sleep(random.uniform(5, 8))
                            curr += timedelta(days=1)
                        elif day_df == "NODATA":
                            st.toast(f"ℹ️ {curr} 證交所確認無交易資料，跳過。")
                            curr += timedelta(days=1)
                        else:
                            st.error(f"❌ {curr} 原始資料下載失敗（遭證交所限制連線）。請更換網路 IP 重試，或切換至右側「手動貼上」模式！")
                            break
                    else:
                        curr += timedelta(days=1)
                    
                    if total_days > 0:
                        progress_val = min(1.0, (curr - start_point).days / total_days)
                        p_bar.progress(progress_val)
                        
                status_text.text("✨ 原始資料更新流程結束！")
                st.rerun()

    elif update_method == "2. 遭阻擋時手動貼上":
        manual_date = st.date_input("要補原始資料的日期", value=(last_date + timedelta(days=1)) if last_date else datetime(2026, 4, 27).date())
        m_date_str = manual_date.strftime('%Y%m%d')
        st.markdown(f"**第一步：** [點我打開證交所 {manual_date} 原始網頁](https://www.twse.com.tw/fund/T86?response=csv&date={m_date_str}&selectType=ALLBUT0999)")
        st.caption("ℹ️ 如果網頁打開有很多密密麻麻的文字，請全部複製。若顯示無資料或訪問控制，請用手機開啟網頁複製。")
        
        raw_text = st.text_area("**第二步：** 請在此處貼上剛才複製的全部文字")
        
        if st.button("第三步：強制手動寫入資料庫", type="inverse", use_container_width=True):
            if raw_text.strip():
                parsed_df = parse_t86_text(raw_text, manual_date)
                if isinstance(parsed_df, pd.DataFrame) and not parsed_df.empty:
                    db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
                    db = pd.concat([db, parsed_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                    db.to_parquet(DATA_FILE, index=False)
                    st.success(f"🔥 成功！已強制寫入 {manual_date} 的原始三大法人資料！")
                    time.sleep(1)
                    st.rerun()
                elif parsed_df == "NODATA":
                    st.warning("證交所文字顯示當天非交易日（無資料），不需寫入。")
                else:
                    st.error("解析失敗！請確認貼上的內容是否完整（需包含『證券代號』等字樣）。")
            else:
                st.error("請先貼上網頁內容再點擊寫入。")

# ====================== 2. 報表顯示 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    latest_db_date = main_db['日期'].max()
    
    if mode == "今日強勢戰報":
        st.info(f"📊 數據基準日：{latest_db_date.date()}")
        db_s = main_db.sort_values(['證券代號', '日期']).copy()
        db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
        db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        today_data = db_s[db_s['日期'] == latest_db_date].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

        with st.spinner("🚀 同步即時報價中..."):
            codes = pre_filter['證券代號'].tolist()
            tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
            price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False)
            res_today = []
            for s in codes:
                for suffix in [".TW", ".TWO"]:
                    t = f"{s}{suffix}"
                    if t in price_data.columns.levels[0]:
                        p_df = price_data[t].dropna()
                        if not p_df.empty:
                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                            row = pre_filter[pre_filter['證券代號']==s].iloc[0]
                            diff_pct = round(((curr - ma5) / ma5 * 100), 2)
                            res_today.append({
                                "代號": s, "名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                "現價": curr, "5日均價": ma5, "價差%": f"{diff_pct}%",
                                "連買": int(row['連續買超']), 
                                "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中",
                                "_sort": 0 if row['連續買超'] == 1 else 1
                            })
                            break
            if res_today:
                df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=[True, False])
                st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)

    elif mode == "籌碼週期分析":
        st.info(f"📊 週期基準日：{latest_db_date.date()}")
        db_c = main_db.sort_values(['證券代號', '日期']).copy()
        db_c['大買'] = db_c['三大法人買賣超股數'] > 3000000 
        db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        active_today = db_c[db_c['日期'] == latest_db_date]
        active_codes = active_today[active_today['連買計數'] >= 1]['證券代號'].unique()
        
        res_cycle = []
        with st.status("🔄 深度分析中...") as status:
            codes = active_codes[:150].tolist()
            if codes:
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                for c in codes:
                    s_data = db_c[db_c['證券代號'] == c].copy()
                    for suf in [".TW", ".TWO"]:
                        t = f"{c}{suf}"
                        if t in p_data_c.columns.levels[0]:
                            p_df = p_data_c[t].dropna()
                            if not p_df.empty:
                                curr = round(float(p_df['Close'].iloc[-1]), 2)
                                ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                
                                last_c = s_data[s_data['日期'] == latest_db_date]['連買計數'].iloc[0]
                                buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                sell_pt = round(curr + (avg_r * 1.6), 2)
                                
                                res_cycle.append({
                                    "代號": c, 
                                    "名稱": s_data['證券名稱'].iloc[0],
                                    "現價": curr, 
                                    "預期價差": round(sell_pt - curr, 2),
                                    "建議買點": buy_pt, 
                                    "預期賣點": sell_pt,
                                    "現差": round(sell_pt - curr, 2),
                                    "連買天數": int(last_c),
                                    "今日狀態": "🟢 剛發動" if last_c <= 2 else f"⚪ 連買 {int(last_c)} 天",
                                    "最佳買日": "🔥 就在今天" if last_c <= 2 else "⏳ 等待回測",
                                    "_sort": 0 if last_c <= 2 else 1,
                                    "_val": round(sell_pt - curr, 2)
                                })
                                break
            status.update(label="✅ 分析完成", state="complete")
        
        if res_cycle:
            df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '_val'], ascending=[True, False])
            st.dataframe(df_cycle.drop(columns=['_sort', '_val']), use_container_width=True, hide_index=True)
else:
    st.warning("請執行數據更新以獲取資料。")
