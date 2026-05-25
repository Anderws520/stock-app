import streamlit as st
import pandas as pd
import numpy as np

# 設定網頁標題與基本配置
st.set_page_config(page_title="三大法人籌碼監控儀表板", page_icon="📈", layout="wide")

# ====================================================================
# 1. 讀取 Google Sheets 資料 (此處請保留你原本的連線與讀取方式)
# ====================================================================
# 範例：df = conn.read(...) 或 df = pd.read_csv(url)
# 這裡為了展示完整邏輯，假設讀進來的 DataFrame 叫 raw_df

# 防呆：確保讀進來有資料，並清除欄位前後的空白字元
if 'raw_df' in locals() or 'df' in locals():
    # 統一將變數名稱指向 df
    if 'raw_df' in locals():
        df = raw_df.copy()
else:
    # 這行是讀取示意，請替換成你原本專案中實際讀取 Google Sheets 的程式碼
    # df = conn.read(ttl="5m") 
    st.warning("⚠️ 請確保此處已正確接入你的 Google Sheets 資料來源。")
    st.stop()

# 強制將所有欄位名稱轉字串並清除隱形空白
df.columns = [str(c).strip() for c in df.columns]

# ====================================================================
# 2. 資料清洗與型態轉換 (徹底根除 KeyError 與排序錯誤)
# ====================================================================
try:
    # 確保數值欄位不會因為文字或逗號導致無法排序
    df["買超張數"] = pd.to_numeric(df["買超張數"].astype(str).str.replace(',', ''), errors='coerce')
    df["目前現價"] = pd.to_numeric(df["目前現價"].astype(str).str.replace(',', ''), errors='coerce')
    df["5日均價"] = pd.to_numeric(df["5日均價"].astype(str).str.replace(',', ''), errors='coerce')
    df["出現天數"] = pd.to_numeric(df["出現天數"], errors='coerce').fillna(1).astype(int)
    
    # 處理價差%，若有 % 符號先拿掉再除以 100
    if "價差%" in df.columns:
        df["價差%"] = df["價差%"].astype(str).str.replace('%', '')
        df["價差%"] = pd.to_numeric(df["價差%"], errors='coerce')
        # 如果數字大於 1，代表原本是 6.00 這種格式，自動轉為 0.06
        df["價差%"] = np.where(df["價差%"] > 1, df["價差%"] / 100, df["價差%"])

    # 確保股票代號格式乾淨（去除單引號）
    df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "")

    # ====================================================================
    # 3. 籌碼資料篩選邏輯
    # ====================================================================
    # 取得最新的一天日期
    if not df.empty:
        latest_date = df["日期"].iloc[0]
    else:
        latest_date = "暫無資料"

    # 篩選出今天最新日期的所有標的
    today_df = df[df["日期"] == latest_date].copy()
    
    # 計算畫面上方顯示的個股統計數量
    total_count = len(today_df)

    # 篩選出要放進 Top 3 的精選標的
    # 對齊你舊試算表中的「超盤建議」（J欄或M欄中包含"強烈推"、"關注"或"今天就是最佳"等字眼）
    if "超盤建議" in today_df.columns:
        # 防呆：先轉字串再篩選
        today_df["超盤建議"] = today_df["超盤建議"].astype(str)
        filtered_for_top3 = today_df[today_df["超盤建議"].str.contains("強烈推|關注|最佳", na=False)].copy()
    else:
        filtered_for_top3 = today_df.copy()

    # 依照「買超張數」由大到小排序，精選前 3 名
    top3 = filtered_for_top3.sort_values(by="買超張數", ascending=False).head(3)

    # ====================================================================
    # 4. Streamlit 前端網頁視覺呈現
    # ====================================================================
    st.title("三大法人籌碼追蹤 · 自動化監控儀表板")
    st.markdown(f"**數據更新日期：`{latest_date}`**")
    
    # 顯示計數區塊
    st.markdown("### 值得關注")
    st.system_font = True
    st.markdown(f"<h1 style='font-size: 54px; margin-top: -20px;'>{total_count}</h1>", unsafe_allow_html=True)
    
    st.write("---")

    # 顯示今日爆買精選 Top 3
    st.subheader(f"🏆 {latest_date} 法人精選 Top 3 飆股")
    if not top3.empty:
        # 只顯示操盤精華欄位，避免畫面太擠
        display_top3 = top3[["股票代號", "股票名稱", "買超張數", "目前現價", "5日均價", "超盤建議"]].copy()
        # 格式化呈現
        display_top3["買超張數"] = display_top3["買超張數"].map('{:,.1f} 張'.format)
        st.dataframe(display_top3, use_container_width=True)
    else:
        st.info("💡 今日暫無符合強烈推薦標準的 Top 3 標的。")

    st.write("---")

    # 顯示全標的詳細清單
    st.subheader(f"📅 {latest_date} 詳細標的清單 (買超 ≥ 500張)")
    if not today_df.empty:
        # 排好順序：日期 -> 買超張數由大到小
        final_list = today_df.sort_values(by="買超張數", ascending=False).copy()
        
        # 格式化百分比與百萬千分位
        if "價差%" in final_list.columns:
            final_list["價差%"] = final_list["價差%"].map('{:.2%}'.format)
        final_list["買超張數"] = final_list["買超張數"].map('{:,.1f}'.format)
        
        # 輸出完整標準 A~J 欄位
        st.dataframe(
            final_list[["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "出現天數", "超盤建議"]],
            use_container_width=True
        )
    else:
        st.warning("查無今日籌碼資料，請確認 Google Apps Script 自動下載是否正常執行。")

except KeyError as e:
    st.error(f"❌ 欄位解析失敗，找不到欄位: {e}")
    st.info("💡 解決辦法：請點擊右上角三點選單點選「Clear cache」，並確認 Google 試算表第一行標頭是否正確。")
except Exception as e:
    st.error(f"應用程式執行時發生非預期錯誤: {e}")
