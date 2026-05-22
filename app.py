import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
import os
import yfinance as yf

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人操盤系統", layout="wide", initial_sidebar_state="collapsed")

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
ADMIN_PASSWORD = "1023520"

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d.strftime('%Y-%m-%d') == "2026-05-01": return False  # 勞動節放假
    return True

@st.cache_data(ttl=86400)
def get_stock_name_map(token=""):
    url = "https://api.finmindtrade.com/api/v4/data"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    parameter = {"dataset": "TaiwanStockInfo"}
    try:
        resp = requests.get(url, headers=headers, params=parameter, timeout=12)
        if resp.status_code == 200:
            data = resp.json().get('data', [])
            df = pd.DataFrame(data)
            if not df.empty and 'stock_id' in df.columns and 'stock_name' in df.columns:
                return dict(zip(df['stock_id'].astype(str), df['stock_name']))
    except Exception as e:
        st.warning(f"⚠️ 無法取得股票名稱對照表: {e}")
    return {}

def download_t86_finmind(target_date, token=""):
    date_str = target_date.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    parameter = {
        "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
        "start_date": date_str,
        "end_date": date_str,
    }
    try:
        resp = requests.get(url, headers=headers, params=parameter, timeout=15)
        if resp.status_code == 403:
            return None
        elif resp.status_code != 200:
            return None
        
        data = resp.json().get('data', [])
        if not data:
            return None
        
        df = pd.DataFrame(data)
        if not {'stock_id', 'buy', 'sell'}.issubset(df.columns):
            return None
        
        df['buy'] = pd.to_numeric(df['buy'], errors='coerce').fillna(0)
        df['sell'] = pd.to_numeric(df['sell'], errors='coerce').fillna(0)
        df['net'] = df['buy'] - df['sell']
        
        df_grouped = df.groupby('stock_id')['net'].sum().reset_index()
        df_grouped['日期'] = pd.to_datetime(target_date)
        df_grouped['stock_id'] = df_grouped['stock_id'].astype(str)
        df_grouped = df_grouped.rename(columns={'stock_id': '證券代號', 'net': '三大法人買賣超股數'})
        
        name_map = get_stock_name_map(token)
        df_grouped['證券名稱'] = df_grouped['證券代號'].map(name_map).fillna("未知")
        
        return df_grouped[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except Exception:
        return None

# ====================== 2. 側邊欄：更新與管理 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    
    finmind_token = st.text_input(
        "FinMind API Token", 
        type="password", 
        help="請至 FinMind 官網註冊獲取免費 Token。"
    )
    
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    last_date = None
    if os.path.exists(DATA_FILE):
        db_info = pd.read_parquet(DATA_FILE)
        if not db_info.empty:
            last_date = pd.to_datetime(db_info['日期']).max().date()
            st.success(f"📁 目前資料庫至：{last_date}")

    if st.button("🔄 自動續傳更新", type="primary", use_container_width=True):
        with st.container():
            db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
            
            # 若無舊資料，預設抓取最近 10 天，避免 API 請求過度超時
            start_point = (last_date + timedelta(days=1)) if last_date else (datetime.now().date() - timedelta(days=10))
            today = datetime.now().date()
            curr = start_point
            
            p_bar = st.progress(0)
            status_text = st.empty()
            total_days = (today - start_point).days + 1
            
            success_count = 0
            fail_count = 0
            
            while curr <= today:
                if is_trading_day(curr):
                    status_text.text(f"⏳ 正在下載 {curr} 籌碼...")
                    day_df = download_t86_finmind(curr, finmind_token)
                    if day_df is not None and not day_df.empty:
                        db = pd.concat([db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        db.to_parquet(DATA_FILE, index=False)
                        success_count += 1
                        status_text.text(f"✅ {curr} 下載並儲存成功！")
                    else:
                        fail_count += 1
                        status_text.text(f"ℹ️ {curr} 無數據或下載失敗。")
                    time.sleep(random.uniform(0.5, 1.0))
                
                curr += timedelta(days=1)
                if total_days > 0:
                    p_bar.progress(min(1.0, (curr - start_point).days / total_days))
            
            if success_count > 0:
                status_text.text(f"🎉 續傳作業結束！成功下載 {success_count} 天資料。")
            else:
                status_text.text(f"⚠️ 作業結束。本次無成功下載新資料 (成功: 0, 失敗/跳過: {fail_count})")
            
            time.sleep(2)
            st.rerun()

# ====================== 3. 報表顯示與分析 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    
    if main_db.empty:
        st.warning("⚠️ 資料庫檔案存在但內容為空，請執行左側「自動續傳更新」。")
    else:
        main_db['日期'] = pd.to_datetime(main_db['日期'])
        latest_db_date = main_db['日期'].max()
        
        if mode == "今日強勢戰報":
            st.info(f"📊 目前資料庫最後更新基準日：{latest_db_date.date()}")
            db_s = main_db.sort_values(['證券代號', '日期']).copy()
            db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
            db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            today_data = db_s[db_s['日期'] == latest_db_date].copy()
            today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
            
            pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

            if pre_filter.empty:
                st.warning(f"⚠️ 在 {latest_db_date.date()} 當天，查無「法人買超 >= 200張」的強勢股。")
            else:
                with st.spinner("🚀 同步即時報價中..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False)
                    res_today = []
                    
                    for s in codes:
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            if isinstance(price_data.columns, pd.MultiIndex) and t in price_data.columns.levels[0]:
                                p_df = price_data[t].dropna()
                                if not p_df.empty:
                                    curr_price = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    row = pre_filter[pre_filter['證券代號'] == s].iloc[0]
                                    diff_pct = round(((curr_price - ma5) / ma5 * 100), 2)
                                    res_today.append({
                                        "代號": s, "名稱": row['證券名稱'], "買超張數": row['買超張數'],
                                        "現價": curr_price, "5日均價": ma5, "價差%": f"{diff_pct}%",
                                        "連買": int(row['連續買超']), 
                                        "操盤建議": "🚀 第一天發動" if row['連續買超'] == 1 else "⏳ 籌碼鎖定中",
                                        "_sort": 0 if row['連續買超'] == 1 else 1
                                    })
                                    break
                                    
                    if res_today:
                        df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=[True, False])
                        st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)
                    else:
                        st.warning("⚠️ 查無即時報價資料。")

        elif mode == "籌碼週期分析":
            st.info(f"📊 目前資料庫最後更新基準日：{latest_db_date.date()}")
            db_c = main_db.sort_values(['證券代號', '日期']).copy()
            db_c['大買'] = db_c['三大法人買賣超股數'] > 3000000 
            db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
            
            active = db_c[db_c['連買計數'] >= 1]['證券代號'].unique()
            res_cycle = []
            
            if len(active) == 0:
                st.warning("⚠️ 目前資料庫中查無符合「連買」條件的標的。")
            else:
                with st.status("🔄 深度分析中...") as status:
                    codes = active[:150].tolist()
                    tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                    p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                    for c in codes:
                        s_data = db_c[db_c['證券代號'] == c].copy()
                        for suf in [".TW", ".TWO"]:
                            t = f"{c}{suf}"
                            if isinstance(p_data_c.columns, pd.MultiIndex) and t in p_data_c.columns.levels[0]:
                                p_df = p_data_c[t].dropna()
                                if not p_df.empty:
                                    curr_price = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                    last_c = s_data['連買計數'].iloc[-1]
                                    buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                    sell_pt = round(curr_price + (avg_r * 1.6), 2)
                                    
                                    res_cycle.append({
                                        "代號": c, "名稱": s_data['證券名稱'].iloc[0],
                                        "現價": curr_price, "預期價差": round(sell_pt - curr_price, 2),
                                        "建議買點": buy_pt, "預期賣點": sell_pt,
                                        "今日狀態": "🟢 剛發動" if last_c <= 2 else f"⚪ 連買 {int(last_c)} 天",
                                        "最佳買日": "🔥 就在今天" if last_c <= 2 else "⏳ 等待回測",
                                        "_sort": 0 if last_c <= 2 else 1,
                                        "_val": round(sell_pt - curr_price, 2)
                                    })
                                    break
                    status.update(label="✅ 分析完成", state="complete")
                
                if res_cycle:
                    df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '_val'], ascending=[True, False])
                    st.dataframe(df_cycle.drop(columns=['_sort', '_val']), use_container_width=True, hide_index=True)
                else:
                    st.warning("⚠️ 分析完畢，但目前無符合條件或報價正常的標的。")

        elif mode == "資料庫管理":
            st.subheader("🗄️ Parquet 本地資料庫狀態")
            st.write(f"資料庫實體路徑： `{DATA_FILE}`")
            total_records = len(main_db)
            unique_dates = main_db['日期'].nunique()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("資料總筆數", f"{total_records:,} 筆")
            with col2:
                st.metric("涵蓋交易日數", f"{unique_dates} 天")
            
            st.write("📋 檢視最末段寫入資料（前 100 筆）：")
            st.dataframe(main_db.tail(100), use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.warning("⚠️ 警告：刪除資料庫將清空所有已下載籌碼資料，需重新進行自動續傳。")
            pwd_input = st.text_input("請輸入管理員密碼確認操作", type="password")
            if st.button("🚨 徹底刪除資料庫檔案", type="secondary"):
                if pwd_input == ADMIN_PASSWORD:
                    os.remove(DATA_FILE)
                    st.success("資料庫刪除成功！")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("密碼錯誤，拒絕清除。")

else:
    st.warning("⚠️ 目前系統找不到資料庫檔案。請執行左側「自動續傳更新」以獲取資料。")
    st.caption("💡 提示：若您部署於免費雲端空間（如 Streamlit Cloud），系統休眠重置時可能會清除歷史檔案，此時需重新點擊更新下載。")
