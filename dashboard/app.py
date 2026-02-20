import os
import time
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "heartbeat_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres123"),
    )


@st.cache_data(ttl=5)
def get_summary_stats():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
                    AVG(heart_rate) as avg_heart_rate,
                    MIN(timestamp) as first_record,
                    MAX(timestamp) as last_record
                FROM heartbeat_records
                WHERE timestamp > NOW() - INTERVAL '1 hour'
            """)
            return cur.fetchone()
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_recent_data(minutes: int = 5):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT customer_id, timestamp, heart_rate, is_anomaly, anomaly_type
                FROM heartbeat_records
                WHERE timestamp > NOW() - INTERVAL '%s minutes'
                ORDER BY timestamp DESC
                LIMIT 1000
            """, (minutes,))
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_anomalies(limit: int = 50):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT customer_id, timestamp, heart_rate, anomaly_type
                FROM heartbeat_records
                WHERE is_anomaly = TRUE
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


@st.cache_data(ttl=5)
def get_customer_data(customer_id: str, minutes: int = 10):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT timestamp, heart_rate, is_anomaly
                FROM heartbeat_records
                WHERE customer_id = %s
                  AND timestamp > NOW() - INTERVAL '%s minutes'
                ORDER BY timestamp
            """, (customer_id, minutes))
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


@st.cache_data(ttl=10)
def get_customer_list():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT customer_id 
                FROM heartbeat_records 
                ORDER BY customer_id
            """)
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def main():
    st.set_page_config(
        page_title="Heartbeat Monitor",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Real-Time Heartbeat Monitoring")

    auto_refresh = st.sidebar.checkbox("Auto-refresh 15s)", value=True)
    if auto_refresh:
        time.sleep(0.1)
        st.rerun()

    try:
        stats = get_summary_stats()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.info("Make sure PostgreSQL is running and the schema is initialized.")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Records (1h)", stats["total_records"] or 0)
    with col2:
        st.metric("Active Customers", stats["unique_customers"] or 0)
    with col3:
        st.metric("Anomalies", stats["anomaly_count"] or 0)
    with col4:
        avg_hr = stats["avg_heart_rate"]
        st.metric("Avg Heart Rate", f"{avg_hr:.1f} BPM" if avg_hr else "N/A")

    st.subheader("Heart Rate Over Time (Last 5 Minutes)")
    recent_data = get_recent_data(5)

    if not recent_data.empty:
        fig = px.scatter(
            recent_data,
            x="timestamp",
            y="heart_rate",
            color="is_anomaly",
            color_discrete_map={True: "red", False: "blue"},
            opacity=0.6,
            labels={"heart_rate": "Heart Rate (BPM)", "timestamp": "Time"},
        )
        fig.add_hline(y=40, line_dash="dash", line_color="orange", annotation_text="Low threshold")
        fig.add_hline(y=150, line_dash="dash", line_color="orange", annotation_text="High threshold")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available yet. Start the producer and consumer.")

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Recent Anomalies")
        anomalies = get_anomalies(20)
        if not anomalies.empty:
            st.dataframe(
                anomalies,
                column_config={
                    "customer_id": "Customer",
                    "timestamp": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
                    "heart_rate": "HR (BPM)",
                    "anomaly_type": "Type",
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No anomalies detected.")

    with col_right:
        st.subheader("Customer Drill-Down")
        customers = get_customer_list()

        if customers:
            selected = st.selectbox("Select Customer", customers)
            customer_data = get_customer_data(selected, 10)

            if not customer_data.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=customer_data["timestamp"],
                    y=customer_data["heart_rate"],
                    mode="lines+markers",
                    name="Heart Rate",
                    line=dict(color="blue"),
                ))

                anomaly_data = customer_data[customer_data["is_anomaly"] == True]
                if not anomaly_data.empty:
                    fig.add_trace(go.Scatter(
                        x=anomaly_data["timestamp"],
                        y=anomaly_data["heart_rate"],
                        mode="markers",
                        name="Anomaly",
                        marker=dict(color="red", size=12),
                    ))

                fig.update_layout(height=300, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No customer data available.")


if __name__ == "__main__":
    main()
