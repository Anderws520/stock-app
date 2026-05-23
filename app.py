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

# ====================== 0. 平台防禦型底層存取器 ======================
def get_item(obj, key):
    """防禦型讀取器，完全取代方括號 [key] 的存取方式"""
    return obj.__getitem__(key)

def set_item(obj, key, val):
    """防禦型指派器，完全取代方括號 [key] = val 的賦值方式"""
    return obj.__setitem__(key, val)

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")

# 使用 list 與 tuple 初始化，100% 避免方括號被平台截斷
USER_AGENTS = list((
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
))
ADMIN_PASSWORD = "1023520"

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d == datetime(2026, 5, 1).date(): return False
    return True

# 🚀 官方 JSON 下載函數 (防禦型重構版)
def force_download(target_date):
    date_str = target_date.strftime('%Y%m%d')
    url = "https://www.twse.com.tw/fund/T86?response=json&date=" + date_str + "&selectType=ALLBUT0999&_=" + str(int(time.time() * 1000))
    
    headers = dict()
    set_item(headers, "User-Agent", random.choice(USER_AGENTS))
    set_item(headers, "Accept", "application/json, text/javascript, */*; q=0.01")
    set_item(headers, "X-Requested-With", "XMLHttpRequest")
    set_item(headers, "Referer", "https://www.twse.com.tw/zh/page/trading/fund/T86.html")
    
    try:
        resp = requests.get(url, headers=headers, timeout=15, verify=False)
        if resp.status_code!= 200 or "html" in resp.text.lower():
            return None
            
        data_json = resp.json()
        if data_json.get('stat')!= 'OK':
            return None
            
        fields = data_json.get('fields', list())
        raw_data = data_json.get('data', list())
        
        if len(fields) == 0 or len(raw_data) == 0:
            return None
            
        df = pd.DataFrame(raw_data, columns=fields)
        df.columns = list(map(lambda c: str(c).strip(), df.columns))
        
        buy_col = next((c for c in df.columns if "三大法人買賣超股數" in c), None)
        if buy_col:
            # 數值清洗與轉換
            net_series = df.__getitem__(buy_col).astype(str).str.replace(',', '', regex=False).apply(pd.to_numeric, errors='coerce').fillna(0)
            set_item(df, '三大法人買賣超股數', net_series)
            set_item(df, '日期', pd.to_datetime(target_date).date())
            
            # 代號提取
            extracted_code = get_item(df.__getitem__('證券代號').astype(str).str.extract(r'(\d+)'), 0)
            set_item(df, '證券代號', extracted_code)
            
            cols_to_keep = list(('日期', '證券代號', '證券名稱', '三大法人買賣超股數'))
            sub_df = df.reindex(columns=cols_to_keep)
            return sub_df.dropna(subset=list(('證券代號',)))
    except Exception:
        return None
    return None

# ====================== 自動斷點續傳更新 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", list(("今日強勢戰報", "籌碼週期分析", "資料庫管理")), index=0)
    st.markdown("---")
    
    if st.button("🔄 自動更新資料（斷點續傳）", type="primary", use_container_width=True):
        with st.spinner("正在執行斷點續傳更新..."):
            if os.path.exists(DATA_FILE):
                db = pd.read_parquet(DATA_FILE)
            else:
                db = pd.DataFrame(columns=list(('日期', '證券代號', '證券名稱', '三大法人買賣超股數')))
            
            if db.empty:
                last_date = datetime(2026, 4, 27).date()
            else:
                last_date = pd.to_datetime(db.__getitem__('日期')).max().date()
            
            today = datetime.now().date()
            target = last_date + timedelta(days=1)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            updated = 0
            total = 40
            
            while target <= today and updated < total:
                if is_trading_day(target):
                    status_text.info("正在抓取 " + str(target) + "...")
                    day_df = force_download(target)
                    if day_df is not None and not day_df.empty:
                        db = pd.concat(list((db, day_df)), ignore_index=True)
                        db = db.drop_duplicates(subset=list(('日期', '證券代號')))
                        db.to_parquet(DATA_FILE, index=False)
                        updated += 1
                        status_text.success("✅ " + str(target) + " 更新成功")
                progress_bar.progress(min(updated / 30, 1.0))
                target += timedelta(days=1)
                time.sleep(random.uniform(5.5, 8.5))
            
            st.success("✅ 斷點續傳完成！本次共更新 " + str(updated) + " 天資料")
            time.sleep(1)
            st.rerun()

    last_d = None
    if os.path.exists(DATA_FILE):
        try:
            db_info = pd.read_parquet(DATA_FILE)
            if not db_info.empty:
                last_d = pd.to_datetime(db_info.__getitem__('日期')).max().date()
                st.success("📁 資料庫最新日期：" + str(last_d))
        except: pass

    if mode == "資料庫管理":
        pwd = st.text_input("管理密碼", type="password")
        if pwd == ADMIN_PASSWORD:
            if st.button("🚨 強制補進 5/4 資料", use_container_width=True):
                target = datetime(2026, 5, 4).date()
                prog_bar = st.progress(0)
                st.info("正在強制抓取 " + str(target) + "...")
                day_df = force_download(target)
                prog_bar.progress(50)
                if day_df is not None:
                    full_db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()
                    full_db = pd.concat(list((full_db, day_df)), ignore_index=True).drop_duplicates(subset=list(('日期', '證券代號')))
                    full_db.to_parquet(DATA_FILE, index=False)
                    prog_bar.progress(100)
                    st.success("✅ 5/4 補帳成功！")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ 抓取失敗")

# ====================== 2. 報表顯示 ======================
st.header("📈 " + mode)

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    set_item(main_db, '日期', pd.to_datetime(main_db.__getitem__('日期')))
    latest_db_date = main_db.__getitem__('日期').max()
    
    if mode == "今日強勢戰報":
        st.info("📊 報表基準日：" + str(latest_db_date.date()))
        db_s = main_db.sort_values(by=list(('證券代號', '日期'))).copy()
        
        # 連續買超計算邏輯
        set_item(db_s, '買超正', db_s.__getitem__('三大法人買賣超股數') > 0)
        continuous_buy = db_s.groupby('證券代號').__getitem__('買超正').transform(lambda x: x * (x.groupby((x!= x.shift()).cumsum()).cumcount() + 1))
        set_item(db_s, '連續買超', continuous_buy)
        
        today_data = db_s.loc[db_s.__getitem__('日期') == latest_db_date].copy()
        set_item(today_data, '買超張數', (today_data.__getitem__('三大法人買賣超股數') / 1000).round(1))
        pre_filter = today_data.loc[today_data.__getitem__('買超張數') >= 200].sort_values(by='買超張數', ascending=False).head(100)

        if pre_filter.empty:
            st.warning("⚠️ 當日尚無符合三大法人買超大於 200 張之強勢標的。")
        else:
            with st.spinner("🚀 即時報價計算中..."):
                codes = pre_filter.__getitem__('證券代號').tolist()
                tickers = list()
                for s in codes:
                    tickers.append(s + ".TW")
                    tickers.append(s + ".TWO")
                    
                price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False)
                res_today = list()
                
                for s in codes:
                    suffixes = list((".TW", ".TWO"))
                    for suffix in suffixes:
                        t = s + suffix
                        if isinstance(price_data.columns, pd.MultiIndex):
                            levels_list = list(price_data.columns.levels)
                            if t in get_item(levels_list, 0):
                                p_df = price_data.__getitem__(t).dropna()
                                if not p_df.empty:
                                    curr = round(float(get_item(p_df.__getitem__('Close'), -1)), 2)
                                    ma5 = round(float(p_df.__getitem__('Close').tail(5).mean()), 2)
                                    
                                    matched_rows = pre_filter.loc[pre_filter.__getitem__('證券代號') == s]
                                    row = matched_rows.iloc.__getitem__(0)
                                    
                                    diff_pct = round(((curr - ma5) / ma5 * 100), 2)
                                    
                                    item = dict()
                                    set_item(item, "代號", s)
                                    set_item(item, "名稱", row.get('證券名稱'))
                                    set_item(item, "買超張數", row.get('買超張數'))
                                    set_item(item, "現價", curr)
                                    set_item(item, "5日均價", ma5)
                                    set_item(item, "價差%", str(diff_pct) + "%")
                                    set_item(item, "連買", int(row.get('連續買超')))
                                    set_item(item, "操盤建議", "🚀 第一天發動" if row.get('連續買超') == 1 else "⏳ 籌碼鎖定中")
                                    set_item(item, "_sort", 0 if row.get('連續買超') == 1 else 1)
                                    res_today.append(item)
                                    break
                if len(res_today) > 0:
                    df_res = pd.DataFrame(res_today).sort_values(by=list(('_sort', '買超張數')), ascending=list((True, False)))
                    st.dataframe(df_res.drop(columns=list(('_sort',))), use_container_width=True, hide_index=True)

    elif mode == "籌碼週期分析":
        st.info("📊 週期基準日：" + str(latest_db_date.date()))
        db_c = main_db.sort_values(by=list(('證券代號', '日期'))).copy()
        
        # 籌碼大買計算
        set_item(db_c, '大買', db_c.__getitem__('三大法人買賣超股數') > 30000)
        continuous_big_buy = db_c.groupby('證券代號').__getitem__('大買').transform(lambda x: x * (x.groupby((x!= x.shift()).cumsum()).cumcount() + 1))
        set_item(db_c, '連買計數', continuous_big_buy)
        
        active_filter = db_c.loc[db_c.__getitem__('連買計數') >= 2]
        active = active_filter.__getitem__('證券代號').unique()
        res_cycle = list()
        
        with st.status("🔄 深度獲利分析中...") as status:
            # 防禦型切片存取：不使用方括號 slice [:150]，改用內建 slice 對象
            codes = get_item(list(active), slice(None, 150))
            if len(codes) > 0:
                tickers = list()
                for s in codes:
                    tickers.append(s + ".TW")
                    tickers.append(s + ".TWO")
                    
                p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                for c in codes:
                    s_data = db_c.loc[db_c.__getitem__('證券代號') == c].copy()
                    suffixes = list((".TW", ".TWO"))
                    for suf in suffixes:
                        t = c + suf
                        if isinstance(p_data_c.columns, pd.MultiIndex):
                            levels_list = list(p_data_c.columns.levels)
                            if t in get_item(levels_list, 0):
                                p_df = p_data_c.__getitem__(t).dropna()
                                if not p_df.empty:
                                    curr = round(float(get_item(p_df.__getitem__('Close'), -1)), 2)
                                    ma5 = round(float(p_df.__getitem__('Close').tail(5).mean()), 2)
                                    
                                    high_low_diff = p_df.__getitem__('High') - p_df.__getitem__('Low')
                                    avg_r = high_low_diff.tail(10).mean()
                                    
                                    last_c = get_item(s_data.__getitem__('連買計數'), -1)
                                    
                                    low_tail3_min = p_df.__getitem__('Low').tail(3).min()
                                    buy_pt = round(min(ma5, low_tail3_min), 2)
                                    sell_pt = round(curr + (avg_r * 1.6), 2)
                                    
                                    item_c = dict()
                                    set_item(item_c, "代號", c)
                                    set_item(item_c, "名稱", get_item(s_data.__getitem__('證券名稱'), 0))
                                    set_item(item_c, "現價", curr)
                                    set_item(item_c, "預期價差", round(sell_pt - curr, 2))
                                    set_item(item_c, "建議買點", buy_pt)
                                    set_item(item_c, "預期賣點", sell_pt)
                                    set_item(item_c, "今日狀態", "🟢 剛發動" if last_c <= 1 else "⏳ 連買 " + str(int(last_c)) + " 天")
                                    set_item(item_c, "最佳買日", "🔥 就在今天" if last_c <= 1 else "⏳ 等待回測")
                                    set_item(item_c, "_sort", 0 if last_c <= 1 else 1)
                                    res_cycle.append(item_c)
                                    break
            status.update(label="✅ 分析完成", state="complete")
        
        if len(res_cycle) > 0:
            df_cycle = pd.DataFrame(res_cycle).sort_values(by=list(('_sort', '預期價差')), ascending=list((True, False)))
            st.dataframe(df_cycle.drop(columns=list(('_sort',))), use_container_width=True, hide_index=True)
else:
    st.warning("目前無資料庫檔案，請點擊側邊欄「自動更新資料」進行首次更新。")
