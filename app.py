import streamlit as st
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
import io
import os
import warnings
warnings.filterwarnings('ignore')

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

# ====================== 0. 全域設定 ======================
st.set_page_config(
    page_title="台股法人操盤系統",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA_FILE = os.path.join(os.getcwd(), "twse_db.parquet")
ADMIN_PASSWORD = "1023520"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# 國定假日（手動維護，避免第三方套件依賴）
TW_HOLIDAYS_2026 = {
    datetime(2026, 1, 1).date(),   # 元旦
    datetime(2026, 1, 26).date(),  # 春節
    datetime(2026, 1, 27).date(),
    datetime(2026, 1, 28).date(),
    datetime(2026, 1, 29).date(),
    datetime(2026, 1, 30).date(),
    datetime(2026, 2, 28).date(),  # 和平紀念日
    datetime(2026, 4, 4).date(),   # 兒童節
    datetime(2026, 4, 3).date(),   # 清明連假
    datetime(2026, 5, 1).date(),   # 勞動節
    datetime(2026, 6, 19).date(),  # 端午節
    datetime(2026, 9, 27).date(),  # 中秋節
    datetime(2026, 10, 9).date(),  # 國慶日連假
    datetime(2026, 10, 10).date(), # 國慶日
}

def is_trading_day(d):
    """判斷是否為交易日"""
    if hasattr(d, 'date'):
        d = d.date()
    if d.weekday() >= 5:
        return False
    if d in TW_HOLIDAYS_2026:
        return False
    return True


# ====================== 1. 核心資料下載（修復版）======================

def clean_number(s):
    """清洗數字字串"""
    if pd.isna(s):
        return 0
    s = str(s).strip().replace(',', '').replace('+', '').replace(' ', '')
    if s in ('', '--', '-', 'N/A', 'nan'):
        return 0
    try:
        return float(s)
    except ValueError:
        return 0

def download_twse_csv(target_date):
    """
    核心修復：改用 CSV 格式下載，並完整處理 BIG5 編碼與欄位解析
    來源：https://www.twse.com.tw/fund/T86
    """
    date_str = target_date.strftime('%Y%m%d')
    
    # 方法 1：官方 CSV 直連（最穩定）
    url = (
        "https://www.twse.com.tw/fund/T86"
        f"?response=csv&date={date_str}&selectType=ALLBUT0999"
    )
    
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.twse.com.tw/zh/page/trading/fund/T86.html",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    
    session = requests.Session()
    
    # 先訪問首頁取得 cookies（模擬真實瀏覽器行為）
    try:
        session.get(
            "https://www.twse.com.tw/zh/page/trading/fund/T86.html",
            headers=headers,
            timeout=10,
            verify=False
        )
        time.sleep(random.uniform(1.5, 2.5))
    except Exception:
        pass
    
    try:
        resp = session.get(url, headers=headers, timeout=20, verify=False)
        
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        
        content = resp.content
        
        # 嘗試 BIG5 解碼（證交所 CSV 為 BIG5 編碼）
        try:
            text = content.decode('big5', errors='replace')
        except Exception:
            try:
                text = content.decode('utf-8', errors='replace')
            except Exception:
                text = content.decode('latin-1', errors='replace')
        
        # 檢查是否為有效資料（非假日、非錯誤頁面）
        if '查詢無資料' in text or 'No Data' in text or len(text.strip()) < 100:
            return None, "查詢無資料（可能為假日或尚未公布）"
        
        if '<html' in text.lower():
            return None, "返回 HTML 非 CSV 格式"
        
        # 解析 CSV
        lines = text.strip().split('\n')
        
        # 找到標題行（包含「證券代號」的行）
        header_idx = None
        for i, line in enumerate(lines):
            if '證券代號' in line and '證券名稱' in line:
                header_idx = i
                break
        
        if header_idx is None:
            return None, "找不到標題行（證券代號/證券名稱）"
        
        # 提取有效資料行
        data_lines = []
        for line in lines[header_idx + 1:]:
            line = line.strip()
            if not line or '合計' in line or line.startswith('"='):
                continue
            # 有效資料行通常以股票代碼開頭
            if line and (line[0].isdigit() or line.startswith('"')):
                data_lines.append(line)
        
        if not data_lines:
            return None, "解析後無有效資料列"
        
        # 讀取標題
        header_line = lines[header_idx].strip()
        
        # 使用 StringIO 重組並用 pandas 解析
        csv_content = header_line + '\n' + '\n'.join(data_lines)
        
        try:
            df = pd.read_csv(
                io.StringIO(csv_content),
                dtype=str,
                na_values=['--', '-', 'N/A', ''],
                keep_default_na=False
            )
        except Exception as e:
            return None, f"pandas 解析失敗：{e}"
        
        # 清洗欄位名稱
        df.columns = [str(c).strip().replace('"', '') for c in df.columns]
        
        # 找三大法人買賣超欄位（可能有不同名稱）
        net_col = None
        for col in df.columns:
            if '三大法人' in col and ('買賣超' in col or '合計' in col):
                net_col = col
                break
        
        if net_col is None:
            # 嘗試找最後一個包含數字的欄位作為買賣超
            return None, f"找不到三大法人買賣超欄位，現有欄位：{list(df.columns)}"
        
        # 找代號欄位
        code_col = None
        for col in df.columns:
            if '代號' in col or '代碼' in col:
                code_col = col
                break
        
        name_col = None
        for col in df.columns:
            if '名稱' in col:
                name_col = col
                break
        
        if code_col is None or name_col is None:
            return None, f"找不到代號或名稱欄位，現有欄位：{list(df.columns)}"
        
        # 建立結果 DataFrame
        result = pd.DataFrame()
        result['證券代號'] = df[code_col].astype(str).str.strip().str.replace('"', '')
        result['證券名稱'] = df[name_col].astype(str).str.strip().str.replace('"', '')
        result['三大法人買賣超股數'] = df[net_col].apply(clean_number)
        result['日期'] = target_date
        
        # 過濾無效代號
        result = result[result['證券代號'].str.match(r'^\d{4,6}$')]
        result = result.dropna(subset=['證券代號'])
        
        if result.empty:
            return None, "過濾後無有效股票資料"
        
        return result, "OK"
    
    except requests.exceptions.Timeout:
        return None, "請求逾時"
    except requests.exceptions.ConnectionError:
        return None, "連線失敗"
    except Exception as e:
        return None, f"未知錯誤：{str(e)}"


def download_with_retry(target_date, max_retries=3):
    """帶重試機制的下載函數"""
    for attempt in range(max_retries):
        if attempt > 0:
            wait = random.uniform(8, 12)
            time.sleep(wait)
        
        df, msg = download_twse_csv(target_date)
        if df is not None:
            return df, "OK"
        
        # 如果是假日/無資料，不重試
        if '查詢無資料' in msg or '假日' in msg:
            return None, msg
    
    return None, f"重試 {max_retries} 次後仍失敗"


# ====================== 2. 資料庫管理 ======================

def load_db():
    if os.path.exists(DATA_FILE):
        try:
            db = pd.read_parquet(DATA_FILE)
            db['日期'] = pd.to_datetime(db['日期'])
            return db
        except Exception:
            pass
    return pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])


def save_db(db):
    db.to_parquet(DATA_FILE, index=False)


def get_missing_dates(db, start_date=None):
    """取得需要補抓的交易日清單"""
    if start_date is None:
        start_date = datetime(2026, 4, 27).date()
    
    if db.empty:
        last_date = start_date - timedelta(days=1)
    else:
        last_date = pd.to_datetime(db['日期']).max().date()
    
    today = datetime.now().date()
    missing = []
    target = last_date + timedelta(days=1)
    
    while target <= today:
        if is_trading_day(target):
            missing.append(target)
        target += timedelta(days=1)
    
    return missing


# ====================== 3. 技術指標計算 ======================

def calc_ma5_from_yf(codes):
    """從 yfinance 批次取得現價與 MA5"""
    if not HAS_YF or not codes:
        return {}
    
    result = {}
    tickers_tw = [c + ".TW" for c in codes]
    tickers_two = [c + ".TWO" for c in codes]
    all_tickers = tickers_tw + tickers_two
    
    try:
        price_data = yf.download(
            all_tickers,
            period="10d",
            interval="1d",
            group_by='ticker',
            progress=False,
            auto_adjust=True
        )
        
        for code in codes:
            for suffix in [".TW", ".TWO"]:
                ticker = code + suffix
                try:
                    if isinstance(price_data.columns, pd.MultiIndex):
                        if ticker not in price_data.columns.get_level_values(0):
                            continue
                        p_df = price_data[ticker].dropna()
                    else:
                        p_df = price_data.dropna()
                    
                    if p_df.empty or len(p_df) < 1:
                        continue
                    
                    close = p_df['Close']
                    curr = float(close.iloc[-1])
                    ma5 = float(close.tail(5).mean())
                    
                    result[code] = {
                        'curr': round(curr, 2),
                        'ma5': round(ma5, 2),
                        'diff_pct': round((curr - ma5) / ma5 * 100, 2) if ma5 > 0 else 0
                    }
                    break
                except Exception:
                    continue
    except Exception:
        pass
    
    return result


def calc_consecutive_buy(db_sorted):
    """計算連續買超天數"""
    db_sorted = db_sorted.copy()
    db_sorted['買超正'] = db_sorted['三大法人買賣超股數'] > 0
    
    def consec(x):
        result = []
        count = 0
        for val in x:
            if val:
                count += 1
            else:
                count = 0
            result.append(count)
        return result
    
    db_sorted['連續買超'] = db_sorted.groupby('證券代號')['買超正'].transform(
        lambda x: pd.Series(consec(x.values), index=x.index)
    )
    return db_sorted


def get_trend_label(buy_qty, consec_days, days_in_db):
    """操盤趨勢診斷"""
    if buy_qty > 1000 and days_in_db < 3:
        return "🔥 雙強初現"
    elif consec_days >= 3:
        return "🔒 法人鎖碼"
    elif consec_days == 1:
        return "🚀 第一天發動"
    else:
        return "⏳ 籌碼鎖定中"


# ====================== 4. 介面 ======================

# CSS 樣式
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0f2027 100%);
        padding: 20px 30px;
        border-radius: 12px;
        margin-bottom: 20px;
        border: 1px solid #00d4ff33;
    }
    .main-header h1 {
        color: #00d4ff;
        font-size: 2rem;
        margin: 0;
        text-shadow: 0 0 20px #00d4ff66;
    }
    .metric-card {
        background: #0f0f23;
        border: 1px solid #1e3a5f;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
    }
    .status-ok { color: #00ff88; font-weight: bold; }
    .status-warn { color: #ffaa00; font-weight: bold; }
    .status-err { color: #ff4444; font-weight: bold; }
    div[data-testid="stDataFrame"] { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>📈 台股法人操盤系統 v2.0</h1>
    <p style="color:#8899aa;margin:0;font-size:0.9rem">三大法人籌碼追蹤 · 均線防護策略 · 斷點續傳資料庫</p>
</div>
""", unsafe_allow_html=True)

# ── 側邊欄 ──
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    
    mode = st.radio(
        "功能切換",
        ["今日強勢戰報", "籌碼週期分析", "資料庫管理"],
        index=0
    )
    
    st.markdown("---")
    
    # 狀態顯示
    db_status = load_db()
    if not db_status.empty:
        last_d = pd.to_datetime(db_status['日期']).max().date()
        total_days = db_status['日期'].nunique()
        total_stocks = db_status['證券代號'].nunique()
        
        st.success(f"📁 最新資料：{last_d}")
        st.info(f"📊 {total_days} 天 · {total_stocks} 檔股票")
        
        missing = get_missing_dates(db_status)
        if missing:
            st.warning(f"⚠️ 缺 {len(missing)} 個交易日")
    else:
        st.error("❌ 尚無資料庫")
    
    st.markdown("---")
    
    # 自動更新按鈕
    if st.button("🔄 自動更新（斷點續傳）", type="primary", use_container_width=True):
        db = load_db()
        missing_dates = get_missing_dates(db)
        
        if not missing_dates:
            st.success("✅ 資料已是最新！")
        else:
            st.info(f"需補抓 {len(missing_dates)} 個交易日")
            progress_bar = st.progress(0)
            status_text = st.empty()
            updated = 0
            failed = []
            
            for i, target in enumerate(missing_dates[:30]):  # 最多補30天
                status_text.info(f"⏳ 抓取 {target}...")
                
                df, msg = download_with_retry(target)
                
                if df is not None and not df.empty:
                    db = pd.concat([db, df], ignore_index=True)
                    db = db.drop_duplicates(subset=['日期', '證券代號'])
                    save_db(db)
                    updated += 1
                    status_text.success(f"✅ {target} 成功 ({len(df)} 檔)")
                else:
                    if '查詢無資料' in msg:
                        status_text.warning(f"🏖️ {target} 休市（{msg}）")
                    else:
                        status_text.error(f"❌ {target} 失敗：{msg}")
                        failed.append(str(target))
                
                progress_bar.progress((i + 1) / len(missing_dates[:30]))
                
                # 請求間隔防封鎖
                if i < len(missing_dates) - 1:
                    time.sleep(random.uniform(5.5, 8.5))
            
            if updated > 0:
                st.success(f"✅ 完成！更新 {updated} 天")
            if failed:
                st.error(f"失敗日期：{', '.join(failed)}")
            
            time.sleep(1)
            st.rerun()
    
    # 資料庫管理模式
    if mode == "資料庫管理":
        st.markdown("---")
        st.subheader("🔧 管理工具")
        pwd = st.text_input("管理密碼", type="password", key="admin_pwd")
        
        if pwd == ADMIN_PASSWORD:
            st.success("✅ 已驗證")
            
            # 手動補抓特定日期
            target_input = st.date_input("指定日期補抓", value=datetime(2026, 5, 4).date())
            
            if st.button("🚨 強制補抓指定日期", use_container_width=True):
                with st.spinner(f"正在抓取 {target_input}..."):
                    df, msg = download_with_retry(target_input)
                    if df is not None:
                        db = load_db()
                        db = pd.concat([db, df], ignore_index=True)
                        db = db.drop_duplicates(subset=['日期', '證券代號'])
                        save_db(db)
                        st.success(f"✅ {target_input} 補帳成功！{len(df)} 筆")
                        st.rerun()
                    else:
                        st.error(f"❌ 失敗：{msg}")
            
            st.markdown("---")
            if st.button("🗑️ 清除全部資料庫", use_container_width=True, type="secondary"):
                if os.path.exists(DATA_FILE):
                    os.remove(DATA_FILE)
                    st.success("已清除")
                    st.rerun()
            
            # 測試下載功能
            st.markdown("---")
            if st.button("🧪 測試下載（昨日）", use_container_width=True):
                test_date = datetime.now().date() - timedelta(days=1)
                while not is_trading_day(test_date):
                    test_date -= timedelta(days=1)
                
                with st.spinner(f"測試下載 {test_date}..."):
                    df, msg = download_twse_csv(test_date)
                    if df is not None:
                        st.success(f"✅ 下載成功！{len(df)} 筆資料")
                        st.dataframe(df.head(10))
                    else:
                        st.error(f"❌ 下載失敗：{msg}")


# ====================== 5. 主要報表 ======================

st.header(f"📊 {mode}")

main_db = load_db()

if main_db.empty:
    st.warning("""
    ⚠️ **尚無資料庫**
    
    請點擊左側「自動更新（斷點續傳）」按鈕開始下載資料。
    
    首次下載可能需要 10-20 分鐘（每筆資料需間隔 5-8 秒防封鎖）。
    """)
    st.stop()

# 設定日期欄位
main_db['日期'] = pd.to_datetime(main_db['日期'])
latest_date = main_db['日期'].max()

# ── 今日強勢戰報 ──
if mode == "今日強勢戰報":
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📅 資料基準日", str(latest_date.date()))
    with col2:
        today_count = len(main_db[main_db['日期'] == latest_date])
        st.metric("📋 今日股票數", f"{today_count} 檔")
    with col3:
        buy_count = len(main_db[(main_db['日期'] == latest_date) & (main_db['三大法人買賣超股數'] > 0)])
        st.metric("📈 法人買超", f"{buy_count} 檔")
    with col4:
        sell_count = len(main_db[(main_db['日期'] == latest_date) & (main_db['三大法人買賣超股數'] < 0)])
        st.metric("📉 法人賣超", f"{sell_count} 檔")
    
    st.markdown("---")
    
    # 計算連續買超
    db_sorted = main_db.sort_values(by=['證券代號', '日期']).copy()
    db_sorted = calc_consecutive_buy(db_sorted)
    
    # 今日資料
    today_data = db_sorted[db_sorted['日期'] == latest_date].copy()
    today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
    
    # 強度門檻：買超 > 500 張（專業操盤邏輯）
    threshold = st.slider("🎚️ 買超張數門檻（張）", 100, 2000, 500, 100)
    
    filtered = today_data[today_data['買超張數'] >= threshold].sort_values(
        by='買超張數', ascending=False
    ).head(100)
    
    if filtered.empty:
        st.warning(f"⚠️ 今日無買超超過 {threshold} 張的標的")
    else:
        st.info(f"✅ 符合條件：{len(filtered)} 檔，正在取得即時報價...")
        
        if HAS_YF:
            codes = filtered['證券代號'].tolist()
            
            with st.spinner("🚀 取得即時報價與 MA5..."):
                price_info = calc_ma5_from_yf(codes)
            
            # 計算各股在資料庫中的天數
            days_in_db = main_db.groupby('證券代號')['日期'].nunique().to_dict()
            
            rows = []
            for _, row in filtered.iterrows():
                code = str(row['證券代號'])
                consec = int(row.get('連續買超', 0))
                buy_qty = float(row['買超張數'])
                days = days_in_db.get(code, 0)
                
                price = price_info.get(code, {})
                curr = price.get('curr', None)
                ma5 = price.get('ma5', None)
                diff_pct = price.get('diff_pct', None)
                
                # 均線防護篩選
                ma5_ok = True
                if curr and ma5 and ma5 > 0:
                    diff = (curr - ma5) / ma5 * 100
                    if diff > 5:  # 乖離超過 5% 視為追高風險
                        ma5_ok = False
                
                trend = get_trend_label(buy_qty, consec, days)
                
                rows.append({
                    "代號": code,
                    "名稱": str(row['證券名稱']),
                    "買超張數": buy_qty,
                    "現價": curr if curr else "取得中",
                    "5日均價": ma5 if ma5 else "計算中",
                    "均線乖離%": f"{diff_pct:+.2f}%" if diff_pct is not None else "-",
                    "連買天數": consec,
                    "均線防護": "✅ 安全" if ma5_ok else "⚠️ 乖離過大",
                    "操盤建議": trend,
                    "_sort_key": (0 if "雙強" in trend or "發動" in trend else 1),
                    "_ma5_ok": ma5_ok,
                })
            
            df_result = pd.DataFrame(rows)
            
            # 顯示分頁：安全 vs 乖離過大
            tab1, tab2 = st.tabs(["✅ 均線安全（建議關注）", "⚠️ 乖離過大（追高風險）"])
            
            with tab1:
                safe = df_result[df_result['_ma5_ok']].sort_values(
                    by=['_sort_key', '買超張數'], ascending=[True, False]
                ).drop(columns=['_sort_key', '_ma5_ok'])
                st.dataframe(safe, use_container_width=True, hide_index=True)
                st.caption(f"共 {len(safe)} 檔（現價在 5 日均線 +5% 以內）")
            
            with tab2:
                risky = df_result[~df_result['_ma5_ok']].sort_values(
                    by='買超張數', ascending=False
                ).drop(columns=['_sort_key', '_ma5_ok'])
                st.dataframe(risky, use_container_width=True, hide_index=True)
                st.caption(f"共 {len(risky)} 檔（乖離過大，謹慎追高）")
        
        else:
            # 沒有 yfinance，顯示基本資料
            display = filtered[['證券代號', '證券名稱', '買超張數', '連續買超']].copy()
            display.columns = ['代號', '名稱', '買超張數', '連買天數']
            st.dataframe(display, use_container_width=True, hide_index=True)
            st.warning("⚠️ 請安裝 yfinance 以顯示即時報價：`pip install yfinance`")

# ── 籌碼週期分析 ──
elif mode == "籌碼週期分析":
    
    st.info(f"📊 週期基準日：{latest_date.date()}")
    
    db_c = main_db.sort_values(by=['證券代號', '日期']).copy()
    
    # 大買定義：買賣超 > 30,000 股（30 張）
    big_buy_threshold = st.slider("🎚️ 大買門檻（張）", 10, 500, 30, 10)
    big_buy_shares = big_buy_threshold * 1000
    
    db_c['大買'] = db_c['三大法人買賣超股數'] > big_buy_shares
    
    def consec_count(x):
        result = []
        count = 0
        for val in x:
            if val:
                count += 1
            else:
                count = 0
            result.append(count)
        return result
    
    db_c['連買計數'] = db_c.groupby('證券代號')['大買'].transform(
        lambda x: pd.Series(consec_count(x.values), index=x.index)
    )
    
    # 取今日連買 >= 2 的股票
    today_cycle = db_c[db_c['日期'] == latest_date].copy()
    active_stocks = today_cycle[today_cycle['連買計數'] >= 2]['證券代號'].tolist()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("🔒 連買 ≥ 2 天標的", f"{len(active_stocks)} 檔")
    with col2:
        strong = today_cycle[today_cycle['連買計數'] >= 5]
        st.metric("💪 連買 ≥ 5 天強勢", f"{len(strong)} 檔")
    
    if not active_stocks:
        st.warning("今日無符合連買條件的標的")
    else:
        st.info(f"找到 {len(active_stocks)} 檔連買標的，分析中...")
        
        if HAS_YF:
            codes = active_stocks[:100]
            
            with st.status("🔄 深度獲利分析中...") as status_widget:
                try:
                    tickers = [c + ".TW" for c in codes] + [c + ".TWO" for c in codes]
                    p_data = yf.download(
                        tickers, period="20d", interval="1d",
                        group_by='ticker', progress=False, auto_adjust=True
                    )
                    status_widget.update(label="✅ 報價取得完成", state="complete")
                except Exception as e:
                    p_data = None
                    status_widget.update(label=f"❌ 報價取得失敗：{e}", state="error")
            
            res_cycle = []
            
            for code in codes:
                s_data = db_c[db_c['證券代號'] == code].copy()
                if s_data.empty:
                    continue
                
                last_consec = int(s_data.sort_values('日期')['連買計數'].iloc[-1])
                stock_name = str(s_data['證券名稱'].iloc[0])
                
                curr, ma5, sell_pt, buy_pt = None, None, None, None
                
                if p_data is not None:
                    for suffix in [".TW", ".TWO"]:
                        ticker = code + suffix
                        try:
                            if isinstance(p_data.columns, pd.MultiIndex):
                                if ticker not in p_data.columns.get_level_values(0):
                                    continue
                                p_df = p_data[ticker].dropna()
                            else:
                                p_df = p_data.dropna()
                            
                            if p_df.empty:
                                continue
                            
                            curr = round(float(p_df['Close'].iloc[-1]), 2)
                            ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                            avg_range = float((p_df['High'] - p_df['Low']).tail(10).mean())
                            low3_min = float(p_df['Low'].tail(3).min())
                            
                            buy_pt = round(min(ma5, low3_min), 2)
                            sell_pt = round(curr + (avg_range * 1.6), 2)
                            break
                        except Exception:
                            continue
                
                days_in_db = len(s_data)
                trend = get_trend_label(
                    float(s_data.sort_values('日期')['三大法人買賣超股數'].iloc[-1]) / 1000,
                    last_consec,
                    days_in_db
                )
                
                res_cycle.append({
                    "代號": code,
                    "名稱": stock_name,
                    "連買天數": last_consec,
                    "現價": curr if curr else "-",
                    "5日均價": ma5 if ma5 else "-",
                    "建議買點": buy_pt if buy_pt else "-",
                    "預期賣點": sell_pt if sell_pt else "-",
                    "預期價差": round(sell_pt - curr, 2) if (sell_pt and curr) else "-",
                    "操盤建議": trend,
                    "最佳買日": "🔥 就在今天" if last_consec <= 2 else "⏳ 等待回測",
                    "_sort": 0 if last_consec <= 2 else 1,
                })
            
            if res_cycle:
                df_cycle = pd.DataFrame(res_cycle).sort_values(
                    by=['_sort', '連買天數'], ascending=[True, False]
                ).drop(columns=['_sort'])
                st.dataframe(df_cycle, use_container_width=True, hide_index=True)
            else:
                st.warning("無法取得報價資料")
        
        else:
            # 不含 yfinance 的基本版
            display_cols = today_cycle[today_cycle['連買計數'] >= 2][
                ['證券代號', '證券名稱', '連買計數', '三大法人買賣超股數']
            ].copy()
            display_cols['買超張數'] = (display_cols['三大法人買賣超股數'] / 1000).round(1)
            display_cols = display_cols.rename(columns={
                '證券代號': '代號', '證券名稱': '名稱', '連買計數': '連買天數'
            }).drop(columns=['三大法人買賣超股數'])
            st.dataframe(display_cols, use_container_width=True, hide_index=True)

# ── 資料庫管理（主畫面）──
elif mode == "資料庫管理":
    
    st.subheader("📊 資料庫狀態總覽")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        earliest = main_db['日期'].min().date()
        st.metric("📅 最早資料", str(earliest))
    with col2:
        latest = main_db['日期'].max().date()
        st.metric("📅 最新資料", str(latest))
    with col3:
        total_d = main_db['日期'].nunique()
        st.metric("📋 總天數", f"{total_d} 天")
    with col4:
        total_s = main_db['證券代號'].nunique()
        st.metric("📈 股票數", f"{total_s} 檔")
    
    st.markdown("---")
    
    # 每日資料量分佈
    daily_count = main_db.groupby('日期').size().reset_index(name='股票數')
    daily_count['日期_str'] = daily_count['日期'].dt.strftime('%m/%d')
    
    st.subheader("📈 每日資料量")
    st.bar_chart(daily_count.set_index('日期_str')['股票數'])
    
    # 缺失日期檢查
    missing = get_missing_dates(main_db)
    if missing:
        st.warning(f"⚠️ 發現 {len(missing)} 個缺失交易日：{[str(d) for d in missing[:10]]}")
    else:
        st.success("✅ 資料完整，無缺失交易日")
    
    # 原始資料預覽
    st.subheader("🔍 原始資料預覽（最新 50 筆）")
    preview = main_db.sort_values('日期', ascending=False).head(50).copy()
    preview['買超張數'] = (preview['三大法人買賣超股數'] / 1000).round(1)
    st.dataframe(preview[['日期', '證券代號', '證券名稱', '買超張數']], use_container_width=True, hide_index=True)
