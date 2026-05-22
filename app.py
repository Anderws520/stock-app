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
    """
    自 FinMind 獲取全市場（包含上市、上櫃）的股票名稱對照表並建立快取，
    避免每次下載法人資料時重複請求而消耗 API 額度。
    """
    url = "https://api.finmindtrade.com/api/v4/data"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    parameter = {"dataset": "TaiwanStockInfo"}
    try:
        resp = requests.get(url, headers=headers, params=parameter, timeout=12)
        if resp.status_code == 200:
            data = resp.json().get('data',)
            df = pd.DataFrame(data)
            if not df.empty and 'stock_id' in df.columns and 'stock_name' in df.columns:
                return dict(zip(df['stock_id'].astype(str), df['stock_name']))
    except Exception as e:
        st.warning(f"⚠️ 無法取得股票名稱對照表（可能無網路或 Token 錯誤）: {e}")
    return {}

def download_t86_finmind(target_date, token=""):
    """
    使用 FinMind API 獲取指定日期全市場的三大法人買賣超數據，並自動合流、轉換欄位。
    """
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
            st.error("🚫 FinMind API 回傳 403 拒絕存取！請確認您的 Token 是否正確，或免費每小時 600 次額度已用完。")
            return None
        elif resp.status_code!= 200:
            return None
        
        res_json = resp.json()
        data = res_json.get('data',)
        if not data:
            return None
        
        df = pd.DataFrame(data)
        if not {'stock_id', 'buy', 'sell'}.issubset(df.columns):
            return None
        
        # 轉換數值並計算三大法人合計淨買超（買進股數 - 賣出股數）
        df['buy'] = pd.to_numeric(df['buy'], errors='coerce').fillna(0)
        df['sell'] = pd.to_numeric(df['sell'], errors='coerce').fillna(0)
        df['net'] = df['buy'] - df['sell']
        
        # 依股票代號加總（因原數據依不同法人分行呈現）
        df_grouped = df.groupby('stock_id')['net'].sum().reset_index()
        df_grouped['日期'] = pd.to_datetime(target_date)
        df_grouped['stock_id'] = df_grouped['stock_id'].astype(str)
        df_grouped = df_grouped.rename(columns={'stock_id': '證券代號', 'net': '三大法人買賣超股數'})
        
        # 進行上市、上櫃名稱映射對照
        name_map = get_stock_name_map(token)
        df_grouped['證券名稱'] = df_grouped['證券代號'].map(name_map).fillna("未知")
        
        return df_grouped[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
    except Exception as e:
        st.error(f"❌ 擷取 {date_str} 資料時發生非預期錯誤: {e}")
        return None

# ====================== 2. 側邊欄：更新與管理 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    
    # 用戶可在此直接輸入 FinMind API 金鑰，提高配額與稳定性
    finmind_token = st.text_input(
        "FinMind API Token", 
        type="password", 
        help="請至 FinMind 官網註冊獲取免費 Token。若留空將以預設無金鑰模式調用。"
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
            start_point = (last_date + timedelta(days=1)) if last_date else datetime(2026, 4, 27).date()
            today = datetime.now().date()
            curr = start_point
            
            p_bar = st.progress(0)
            status_text = st.empty()
            
            total_days = (today - start_point).days + 1
            
            while curr <= today:
                if is_trading_day(curr):
                    status_text.text(f"⏳ 正在向 FinMind 下載 {curr} 的三大法人籌碼...")
                    day_df = download_t86_finmind(curr, finmind_token)
                    if day_df is not None and not day_df.empty:
                        db = pd.concat([db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        db.to_parquet(DATA_FILE, index=False)
                        status_text.text(f"✅ {curr} 下載完成並儲存成功！")
                    else:
                        status_text.text(f"ℹ️ {curr} 無交易數據或下載失敗，跳過。")
                    
                    # API 串接不需向原網頁爬蟲般延遲 3 ~ 5 秒。設為 0.5 ~ 1.0 秒即可。
                    time.sleep(random.uniform(0.5, 1.0))
                
                curr += timedelta(days=1)
                if total_days > 0:
                    p_bar.progress(min(1.0, (curr - start_point).days / total_days))
            
            status_text.text("🎉 自動續傳更新完成！")
            st.rerun()

# ====================== 3. 報表顯示與分析 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    main_db = pd.read_parquet(DATA_FILE)
    main_db['日期'] = pd.to_datetime(main_db['日期'])
    latest_db_date = main_db['日期'].max()
    
    if mode == "今日強勢戰報":
        st.info(f"📊 數據基準日：{latest_db_date.date()}")
        db_s = main_db.sort_values(['證券代號', '日期']).copy()
        db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
        db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x!= x.shift()).cumsum()).cumcount() + 1))
        today_data = db_s[db_s['日期'] == latest_db_date].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

        with st.spinner("🚀 同步即時報價中..."):
            codes = pre_filter['證券代號'].tolist()
            tickers = +
            price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False)
            res_today =
            for s in codes:
                for suffix in:
                    t = f"{s}{suffix}"
                    if t in price_data.columns.levels:
                        p_df = price_data[t].dropna()
                        if not p_df.empty:
                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                            row = pre_filter[pre_filter['證券代號'] == s].iloc
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
                df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=)
                st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)

    elif mode == "籌碼週期分析":
        st.info(f"📊 週期基準日：{latest_db_date.date()}")
        db_c = main_db.sort_values(['證券代號', '日期']).copy()
        db_c['大買'] = db_c['三大法人買賣超股數'] > 3000000 
        db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x!= x.shift()).cumsum()).cumcount() + 1))
        # 篩選出有連買過的標的
        active = db_c[db_c['連買計數'] >= 1]['證券代號'].unique()
        res_cycle =
        
        with st.status("🔄 深度分析中...") as status:
            codes = active[:150].tolist()
            if codes:
                tickers = +
                p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                for c in codes:
                    s_data = db_c[db_c['證券代號'] == c].copy()
                    for suf in:
                        t = f"{c}{suf}"
                        if t in p_data_c.columns.levels:
                            p_df = p_data_c[t].dropna()
                            if not p_df.empty:
                                curr = round(float(p_df['Close'].iloc[-1]), 2)
                                ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                last_c = s_data['連買計數'].iloc[-1]
                                buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                sell_pt = round(curr + (avg_r * 1.6), 2)
                                
                                res_cycle.append({
                                    "代號": c, "名稱": s_data['證券名稱'].iloc,
                                    "現價": curr, "預期價差": round(sell_pt - curr, 2),
                                    "建議買點": buy_pt, "預期賣點": sell_pt,
                                    "現差": round(sell_pt - curr, 2),
                                    "今日狀態": "🟢 剛發動" if last_c <= 2 else f"⚪ 連買 {int(last_c)} 天",
                                    "最佳買日": "🔥 就在今天" if last_c <= 2 else "⏳ 等待回測",
                                    "_sort": 0 if last_c <= 2 else 1,
                                    "_val": round(sell_pt - curr, 2) # 用於輔助排序的數值
                                })
                                break
            status.update(label="✅ 分析完成", state="complete")
        
        if res_cycle:
            df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '_val'], ascending=)
            st.dataframe(df_cycle.drop(columns=['_sort', '_val']), use_container_width=True, hide_index=True)

    elif mode == "資料庫管理":
        st.subheader("🗄️ Parquet 本地資料庫狀態")
        st.write(f"資料庫實體路徑： `{DATA_FILE}`")
        if not main_db.empty:
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
            st.warning("⚠️ 警告：刪除資料庫將清空所有已下載籌碼資料，需重新進行「自動續傳更新」。")
            
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
            st.info("目前尚無有效籌碼資料，請執行側邊欄更新按鈕。")
else:
    st.warning("請執行「自動續傳更新」以獲取資料。")
