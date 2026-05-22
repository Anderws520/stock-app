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

def download_t86_official(target_date):
    """
    直接從證交所(TWSE)與櫃買中心(TPEx)官方Web-JSON接口下載全市場法人數據。
    不需任何API Token，補全Headers防禦403阻擋。
    """
    date_str = target_date.strftime('%Y%m%d')
    # 櫃買中心使用民國曆格式 (例如 115/05/14)
    roc_year = target_date.year - 1911
    date_slash = f"{roc_year}/{target_date.strftime('%m/%d')}"
    
    twse_df = None
    tpex_df = None
    
    # 構造強健的瀏覽器 Headers 防禦 WAF 阻擋
    headers_base = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive'
    }
    
    # --- 1. 擷取上市股票三大法人資料 (TWSE) ---
    twse_url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999"
    try:
        twse_headers = headers_base.copy()
        twse_headers = 'https://www.twse.com.tw/zh/page/trading/fund/T86.html'
        
        resp = requests.get(twse_url, headers=twse_headers, timeout=15)
        if resp.status_code == 200:
            res_json = resp.json()
            if res_json.get('stat') == 'OK' and 'data' in res_json:
                fields = res_json['fields']
                data = res_json['data']
                
                # 動態定位欄位索引以防格式修改
                try:
                    col_code = fields.index('證券代號')
                    col_name = fields.index('證券名稱')
                    col_net = fields.index('三大法人買賣超股數')
                except ValueError:
                    col_code, col_name, col_net = 0, 1, -1
                
                rows =
                for item in data:
                    code = str(item[col_code]).strip().replace('"', '')
                    name = str(item[col_name]).strip()
                    net_str = str(item[col_net]).replace(',', '')
                    try:
                        net_val = float(net_str)
                    except ValueError:
                        net_val = 0.0
                    
                    rows.append({
                        '日期': pd.to_datetime(target_date),
                        '證券代號': code,
                        '證券名稱': name,
                        '三大法人買賣超股數': net_val
                    })
                if rows:
                    twse_df = pd.DataFrame(rows)
    except Exception:
        pass
        
    # --- 2. 擷取上櫃股票三大法人資料 (TPEx) ---
    tpex_url = f"https://www.tpex.org.tw/web/stock/3and5hist/3and5ago/3insti_details.php?l=zh-tw&d={date_slash}&se=EW&t=D"
    try:
        tpex_headers = headers_base.copy()
        tpex_headers = 'https://www.tpex.org.tw/web/stock/3and5hist/3and5ago/3insti_ago.php'
        
        resp = requests.get(tpex_url, headers=tpex_headers, timeout=15)
        if resp.status_code == 200:
            res_json = resp.json()
            if 'aaData' in res_json and res_json:
                data = res_json
                rows =
                for item in data:
                    if len(item) < 3: continue
                    code = str(item).strip().replace('"', '')
                    name = str(item[3]).strip()
                    # 櫃買中心最後一欄(或倒數第二欄)為三大法人合計買賣超股數
                    net_str = str(item[-1]).replace(',', '')
                    try:
                        net_val = float(net_str)
                    except ValueError:
                        net_val = 0.0
                    
                    rows.append({
                        '日期': pd.to_datetime(target_date),
                        '證券代號': code,
                        '證券名稱': name,
                        '三大法人買賣超股數': net_val
                    })
                if rows:
                    tpex_df = pd.DataFrame(rows)
    except Exception:
        pass
        
    # --- 3. 合併數據流 ---
    if twse_df is not None and tpex_df is not None:
        return pd.concat([twse_df, tpex_df], ignore_index=True)
    elif twse_df is not None:
        return twse_df
    elif tpex_df is not None:
        return tpex_df
    return None

# ====================== 2. 側邊欄：更新與管理 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    st.info("💡 目前版本已切換至官方免費通道，無需配置 Token！")
    
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")
    
    last_date = None
    if os.path.exists(DATA_FILE):
        try:
            db_info = pd.read_parquet(DATA_FILE)
            if not db_info.empty:
                last_date = pd.to_datetime(db_info['日期']).max().date()
                st.success(f"📁 目前資料庫至：{last_date}")
        except Exception:
            st.warning("⚠️ 資料庫損毀，請執行下方更新重設。")

    if st.button("🔄 自動續傳更新", type="primary", use_container_width=True):
        with st.container():
            db = pd.read_parquet(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
            
            # 若無舊資料，預設自動回溯 10 天
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
                    status_text.text(f"⏳ 正在下載 {curr} 籌碼數據...")
                    day_df = download_t86_official(curr)
                    
                    if day_df is not None and not day_df.empty:
                        db = pd.concat([db, day_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        db.to_parquet(DATA_FILE, index=False)
                        success_count += 1
                        status_text.text(f"✅ {curr} 下載並合併儲存成功！")
                    else:
                        fail_count += 1
                        status_text.text(f"ℹ️ {curr} 為非交易日或伺服器尚未更新，跳過。")
                    
                    # 避免頻率過高被伺服器阻擋，隨機延遲
                    time.sleep(random.uniform(2.0, 3.5))
                
                curr += timedelta(days=1)
                if total_days > 0:
                    p_bar.progress(min(1.0, (curr - start_point).days / total_days))
            
            if success_count > 0:
                status_text.text(f"🎉 續傳作業結束！本次成功下載 {success_count} 天新資料。")
            else:
                status_text.text(f"⚠️ 作業結束。本次無新交易日可供更新 (成功: 0, 跳過: {fail_count})")
            
            time.sleep(2)
            st.rerun()

# ====================== 3. 報表顯示與分析 ======================
st.header(f"📈 {mode}")

if os.path.exists(DATA_FILE):
    try:
        main_db = pd.read_parquet(DATA_FILE)
    except Exception:
        main_db = pd.DataFrame()
    
    if main_db.empty:
        st.warning("⚠️ 目前系統中尚無資料，請執行左側「自動續傳更新」。")
    else:
        main_db['日期'] = pd.to_datetime(main_db['日期'])
        latest_db_date = main_db['日期'].max()
        
        if mode == "今日強勢戰報":
            st.info(f"📊 歷史最後更新基準日：{latest_db_date.date()}")
            db_s = main_db.sort_values(['證券代號', '日期']).copy()
            db_s['買超正'] = db_s['三大法人買賣超股數'] > 0
            db_s['連續買超'] = db_s.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x!= x.shift()).cumsum()).cumcount() + 1))
            today_data = db_s[db_s['日期'] == latest_db_date].copy()
            today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
            
            pre_filter = today_data[today_data['買超張數'] >= 200].sort_values('買超張數', ascending=False).head(100)

            if pre_filter.empty:
                st.warning(f"⚠️ 在最後基準日 {latest_db_date.date()}，市場上無符合「法人買超 >= 200張」的強勢股。")
            else:
                with st.spinner("🚀 同步即時報價中..."):
                    codes = pre_filter['證券代號'].tolist()
                    tickers = +
                    price_data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', progress=False)
                    res_today =
                    
                    for s in codes:
                        for suffix in:
                            t = f"{s}{suffix}"
                            if isinstance(price_data.columns, pd.MultiIndex) and t in price_data.columns.levels:
                                p_df = price_data[t].dropna()
                                if not p_df.empty:
                                    curr_price = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    row = pre_filter[pre_filter['證券代號'] == s].iloc
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
                        df_res = pd.DataFrame(res_today).sort_values(['_sort', '買超張數'], ascending=)
                        st.dataframe(df_res.drop(columns=['_sort']), use_container_width=True, hide_index=True)
                    else:
                        st.warning("⚠️ 查無即時報價資料，請檢查網路連線。")

        elif mode == "籌碼週期分析":
            st.info(f"📊 歷史最後更新基準日：{latest_db_date.date()}")
            db_c = main_db.sort_values(['證券代號', '日期']).copy()
            db_c['大買'] = db_c['三大法人買賣超股數'] > 3000000 
            db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(lambda x: x * (x.groupby((x!= x.shift()).cumsum()).cumcount() + 1))
            
            active = db_c[db_c['連買計數'] >= 1]['證券代號'].unique()
            res_cycle =
            
            if len(active) == 0:
                st.warning("⚠️ 目前歷史資料庫中無符合「大買 3000 張以上」連買條件的標的。")
            else:
                with st.status("🔄 深度分析中...") as status:
                    codes = active[:150].tolist()
                    tickers = +
                    p_data_c = yf.download(tickers, period="20d", interval="1d", group_by='ticker', progress=False)
                    for c in codes:
                        s_data = db_c[db_c['證券代號'] == c].copy()
                        for suf in:
                            t = f"{c}{suf}"
                            if isinstance(p_data_c.columns, pd.MultiIndex) and t in p_data_c.columns.levels:
                                p_df = p_data_c[t].dropna()
                                if not p_df.empty:
                                    curr_price = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                    last_c = s_data['連買計數'].iloc[-1]
                                    buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                    sell_pt = round(curr_price + (avg_r * 1.6), 2)
                                    
                                    res_cycle.append({
                                        "代號": c, "名稱": s_data['證券名稱'].iloc,
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
                    df_cycle = pd.DataFrame(res_cycle).sort_values(['_sort', '_val'], ascending=)
                    st.dataframe(df_cycle.drop(columns=['_sort', '_val']), use_container_width=True, hide_index=True)
                else:
                    st.warning("⚠️ 分析完畢，但無符合條件或報價正常的標的。")

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
            
            st.write("📋 檢視歷史寫入資料（前 100 筆）：")
            st.dataframe(main_db.tail(100), use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.warning("⚠️ 警告：刪除資料庫將清空所有已下載籌碼資料。")
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
    st.warning("⚠️ 目前系統中尚無 parquet 歷史資料庫檔案。請點擊左側「自動續傳更新」以開始下載。")
    st.caption("💡 提示：若您部署於免費雲端空間（如 Streamlit Cloud），系統休眠重置時會清除歷史暫存檔，此時僅需再次點擊更新即可重新載入最新的資料。")
