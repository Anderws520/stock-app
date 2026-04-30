import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="法人真愛分析", layout="wide")
st.title("🚀 法人真愛股 - 日期自選版")

GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

# --- 側邊欄：從拉霸改成日期輸入框 ---
st.sidebar.header("📅 查詢區間設定")
# 預設結束日期為今天，開始日期為 7 天前
start_date = st.sidebar.date_input("開始日期", datetime.now() - timedelta(days=7))
end_date = st.sidebar.date_input("結束日期", datetime.now())

if start_date > end_date:
    st.sidebar.error("錯誤：開始日期不能晚於結束日期")

def get_data(date_str):
    try:
        resp = requests.get(f"{GAS_URL}?date={date_str}", timeout=20)
        if resp.status_code == 200:
            json_data = resp.json()
            if json_data.get('stat') == 'OK':
                df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
                df['日期'] = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
                return df
    except: return None
    return None

if st.button("🔍 依照日期區間執行同步"):
    # 生成日期清單
    date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()
    date_list.reverse() # 從最新的開始抓
    
    all_data_list = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with st.spinner("正在穿越時空抓取數據..."):
        for i, d_str in enumerate(date_list):
            status_text.text(f"正在同步 {d_str} 的資料...")
            df = get_data(d_str)
            if df is not None:
                all_data_list.append(df)
            
            # 更新進度條
            progress_bar.progress((i + 1) / len(date_list))
            # 透過 Google 抓，速度可以快一點
            time.sleep(0.2)
            
        if len(all_data_list) >= 1:
            full_df = pd.concat(all_data_list)
            
            # 轉換數字並計算張數
            for col in ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']:
                full_df[col] = full_df[col].str.replace(',','').astype(float)
            
            full_df['買超張數'] = (full_df['外陸資買賣超股數(不含外資自營商)'] + 
                               full_df['投信買賣超股數'] + 
                               full_df['自營商買賣超股數']) / 1000
            
            # 計算在這個區間內，股票出現過幾次
            counts = full_df.groupby('證券代號').size().to_dict()
            full_df['連續出現天數'] = full_df['證券代號'].map(counts)
            
            # 格式復刻
            excel_df = pd.DataFrame({
                '日期': full_df['日期'],
                '股票代號': full_df['證券代號'],
                '股票名稱': full_df['證券名稱'],
                '關鍵分點': '三大法人',
                '買超張數': full_df['買超張數'].round(0),
                '5日均價': '-', 
                '目前現價': '-', 
                '價差%': '-',   
                '連續出現天數': full_df['連續出現天數'],
                '集保人數變動': '無數據'
            })
            
            excel_df = excel_df.sort_values(by=['日期', '買超張數'], ascending=[False, False])
            
            st.success(f"同步完成！共分析 {len(date_list)} 天，找到 {len(excel_df)} 筆紀錄。")
            st.dataframe(excel_df, use_container_width=True, hide_index=True)
        else:
            st.error("所選區間內抓不到資料，請確認是否包含交易日。")
