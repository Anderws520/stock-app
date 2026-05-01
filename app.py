import streamlit as st
import pandas as pd
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import re

st.set_page_config(page_title="台股法人除錯工具", layout="wide")
st.title("🛠️ 台股三大法人抓取 - 除錯專用版")
st.markdown("**專門解決黑屏與 SSL 問題**")

# 配置
DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 4, 27).date()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
]

def is_trading_day(d):
    if d.weekday() >= 5:
        return False
    if d == datetime(2026, 5, 1).date():
        return False
    return True

def get_url(date):
    return f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"

def clean_number(x):
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x)
    try:
        return float(x)
    except:
        return None

# ==================== 主測試函數 ====================
def test_download_one_day(test_date):
    url = get_url(test_date)
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    st.info(f"正在測試 {test_date} ...")
    st.caption(f"URL: {url}")
    
    try:
        # 強制關閉 SSL 驗證 + 較長 timeout
        resp = requests.get(url, headers=headers, timeout=30, verify=False)
        st.success(f"HTTP 狀態碼: {resp.status_code}")
        
        if resp.status_code != 200:
            st.error(f"狀態碼錯誤: {resp.status_code}")
            return None
            
        text = resp.text
        st.caption(f"回應內容長度: {len(text)} 字元")
        
        # 顯示前 500 字元幫助除錯
        st.text_area("回應內容前500字元", text[:500], height=150)
        
        # 找資料起始行
        lines = text.splitlines()
        start_idx = None
        for i, line in enumerate(lines):
            if "證券代號" in line:
                start_idx = i
                break
                
        if start_idx is None:
            st.error("找不到『證券代號』這行文字")
            return None
            
        csv_text = "\n".join(lines[start_idx:])
        df = pd.read_csv(StringIO(csv_text), encoding='big5', on_bad_lines='skip')
        
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        
        st.success(f"✅ 成功解析！抓到 {len(df)} 筆資料")
        st.write("欄位名稱：", list(df.columns))
        
        return df
        
    except Exception as e:
        st.error(f"❌ 發生錯誤: {type(e).__name__}")
        st.error(str(e))
        return None

# ====================== UI ======================
st.subheader("單日測試（推薦先測今天）")

col1, col2 = st.columns(2)
with col1:
    test_date = st.date_input("選擇測試日期", value=datetime.now().date())
with col2:
    if st.button("🚀 測試單日抓取", type="primary"):
        with st.spinner("正在連線證交所..."):
            result = test_download_one_day(test_date)

st.markdown("---")
if st.button("開始完整更新資料（從2026-4-27開始）"):
    st.warning("請先確認單日測試成功後再執行完整更新，避免再次黑屏")

st.caption("如果還是黑屏，請把終端機（命令提示字元）中顯示的錯誤完整複製貼給我。")