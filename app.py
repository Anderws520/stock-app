# --- 在顯示表格前加入這段防護 ---
try:
    if not final_df.empty:
        st.dataframe(final_df, use_container_width=True, hide_index=True)
    else:
        st.info("目前無符合條件之標的。")
except Exception as e:
    st.error(f"畫面渲染失敗，原因：{e}")
    st.info("建議至資料庫管理執行『全部重頭下載』以修復欄位衝突。")
