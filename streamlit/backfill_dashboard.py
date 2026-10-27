"""
Airport Operations Intelligence Dashboard
==========================================
Stakeholder: Airport Operations Director
Purpose: Capacity planning, peak management, airline risk, and peer benchmarking.
Powered by the BACKFILL schema (7-day clean dataset).

Run with:  streamlit run backfill_dashboard.py
"""

import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Airport Operations Intelligence",
    page_icon="🛫",
    layout="wide",
)

TEMPLATE = "plotly_white"

# ── Snowflake connection ──────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="BACKFILL",
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=120,
        network_timeout=120,
    )


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        columns = [desc[0].lower() for desc in cur.description]
        data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cur.close()


# ── Queries ───────────────────────────────────────────────────────────

Q_AIRPORT_SUMMARY = """
SELECT airport_icao, city,
       SUM(total_arrivals) AS arrivals,
       SUM(total_departures) AS departures,
       SUM(total_movements) AS total_movements,
       ROUND(AVG(total_movements), 0) AS avg_daily,
       MIN(flight_date) AS data_from,
       MAX(flight_date) AS data_to,
       COUNT(DISTINCT flight_date) AS days
FROM gld_daily_airport_traffic
WHERE flight_date < (SELECT MAX(flight_date) FROM gld_daily_airport_traffic)
GROUP BY 1, 2
ORDER BY total_movements DESC
"""

Q_DAILY_TREND = """
SELECT flight_date, airport_icao, city,
       total_arrivals, total_departures, total_movements
FROM gld_daily_airport_traffic
WHERE flight_date < (SELECT MAX(flight_date) FROM gld_daily_airport_traffic)
ORDER BY flight_date, airport_icao
"""

Q_HOURLY = """
SELECT airport_icao, city, departure_hour_local,
       SUM(flight_count) AS flights
FROM gld_hourly_flight_activity
WHERE flight_date < (SELECT MAX(flight_date) FROM gld_daily_airport_traffic)
GROUP BY 1, 2, 3
ORDER BY 1, 3
"""

Q_AIRLINE_BY_AIRPORT = """
SELECT airport_icao, city, airline_name, alliance,
       SUM(total_flights) AS flights
FROM gld_airline_traffic
WHERE airline_name IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY airport_icao, flights DESC
"""

Q_TOP_ROUTES_BY_AIRPORT = """
SELECT departure_airport_icao, arrival_airport_icao,
       departure_city, arrival_city,
       total_flights,
       ROUND(avg_duration_min, 0) AS avg_duration_min,
       unique_aircraft,
       days_with_service
FROM gld_route_analysis
ORDER BY total_flights DESC
"""

# ── Load all data ─────────────────────────────────────────────────────
airport_df = run_query(Q_AIRPORT_SUMMARY)
daily_df = run_query(Q_DAILY_TREND)
hourly_df = run_query(Q_HOURLY)
airline_df = run_query(Q_AIRLINE_BY_AIRPORT)
routes_df = run_query(Q_TOP_ROUTES_BY_AIRPORT)

# ── Header ────────────────────────────────────────────────────────────
st.title("Airport Operations Intelligence")
st.caption(
    f"Reporting period: {airport_df['data_from'].iloc[0]} to {airport_df['data_to'].iloc[0]} "
    f"| {int(airport_df['days'].iloc[0])} days | Source: OpenSky Network"
)

# ── Airport selector ──────────────────────────────────────────────────
airport_options = airport_df["airport_icao"].tolist()
city_map = dict(zip(airport_df["airport_icao"], airport_df["city"]))

selected = st.selectbox(
    "Select your airport",
    options=airport_options,
    format_func=lambda x: f"{x} — {city_map[x]}",
)

my = airport_df[airport_df["airport_icao"] == selected].iloc[0]
peers = airport_df[airport_df["airport_icao"] != selected]

st.divider()

# =====================================================================
# SECTION 1: CAPACITY OVERVIEW
# =====================================================================
st.header("1 — Capacity Overview")

# KPIs for selected airport
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Movements", f"{int(my['total_movements']):,}")
k2.metric("Avg Daily", int(my["avg_daily"]))
k3.metric("Arrivals", f"{int(my['arrivals']):,}")
k4.metric("Departures", f"{int(my['departures']):,}")

# Arrival/departure imbalance
imbalance = int(my["departures"]) - int(my["arrivals"])
imbalance_pct = round(imbalance * 100 / my["total_movements"], 1) if my["total_movements"] > 0 else 0
k5.metric(
    "Arr/Dep Imbalance",
    f"{imbalance_pct:+.1f}%",
    delta=f"{imbalance:+d} flights",
    delta_color="off",
)

# Recommendation
if abs(imbalance_pct) > 3:
    direction = "departures" if imbalance > 0 else "arrivals"
    st.info(
        f"**Operational note:** {city_map[selected]} shows a {abs(imbalance_pct)}% skew toward "
        f"{direction}. This may indicate aircraft repositioning pressure — review gate "
        f"allocation and turnaround scheduling."
    )
else:
    st.success(
        f"**Balanced operations:** {city_map[selected]} arrivals and departures are well balanced "
        f"({abs(imbalance_pct)}% variance). No gate reallocation needed."
    )

st.divider()

# =====================================================================
# SECTION 2: DAILY VOLUME TREND + PEER BENCHMARK
# =====================================================================
st.header("2 — Daily Volume Trend & Peer Benchmark")

col_trend, col_bench = st.columns([3, 2])

with col_trend:
    my_daily = daily_df[daily_df["airport_icao"] == selected].copy()

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=my_daily["flight_date"], y=my_daily["total_movements"],
        name=city_map[selected], mode="lines+markers",
        line=dict(color="#636EFA", width=3),
    ))
    # Add peer average as reference
    peer_avg = daily_df[daily_df["airport_icao"] != selected].groupby("flight_date")["total_movements"].mean().reset_index()
    fig_trend.add_trace(go.Scatter(
        x=peer_avg["flight_date"], y=peer_avg["total_movements"],
        name="Peer Average", mode="lines",
        line=dict(color="#CCCCCC", dash="dash", width=2),
    ))
    fig_trend.update_layout(
        template=TEMPLATE,
        xaxis_title="", yaxis_title="Daily Movements",
        legend=dict(orientation="h", y=-0.2),
        height=350, margin=dict(t=10),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with col_bench:
    # Peer ranking bar
    all_airports = airport_df[["city", "avg_daily"]].copy()
    all_airports["is_selected"] = all_airports["city"] == city_map[selected]
    all_airports = all_airports.sort_values("avg_daily", ascending=True)

    colors = ["#636EFA" if s else "#D3D3D3" for s in all_airports["is_selected"]]

    fig_rank = go.Figure(go.Bar(
        x=all_airports["avg_daily"],
        y=all_airports["city"],
        orientation="h",
        marker_color=colors,
    ))
    fig_rank.update_layout(
        template=TEMPLATE,
        xaxis_title="Avg Daily Movements",
        yaxis_title="",
        height=350, margin=dict(t=10),
    )
    st.plotly_chart(fig_rank, use_container_width=True)

# Trend recommendation
my_volumes = my_daily["total_movements"].tolist()
if len(my_volumes) >= 3:
    recent_avg = np.mean(my_volumes[-3:])
    earlier_avg = np.mean(my_volumes[:3])
    trend_pct = round((recent_avg - earlier_avg) / earlier_avg * 100, 1) if earlier_avg > 0 else 0
    peer_daily_avg = int(peer_avg["total_movements"].mean())

    if len(my_volumes) >= 14 and trend_pct > 5:
        st.warning(
            f"**Capacity alert:** {city_map[selected]} shows a **{trend_pct:+.1f}% upward trend** "
            f"over the period. If sustained, consider reviewing runway slot limits and "
            f"staffing levels for the coming weeks."
        )
    elif len(my_volumes) >= 14 and trend_pct < -5:
        st.info(
            f"**Volume declining:** {city_map[selected]} traffic is trending **{trend_pct:+.1f}%**. "
            f"Evaluate whether this is seasonal or structural. Review airline schedule "
            f"commitments for the next period."
        )

    rank = int((airport_df["avg_daily"] >= my["avg_daily"]).sum())
    st.caption(
        f"Peer benchmark: {city_map[selected]} ranks **#{rank}** of 6 airports "
        f"with {int(my['avg_daily'])} avg daily movements vs peer average of {peer_daily_avg}."
    )

st.divider()

# =====================================================================
# SECTION 3: PEAK HOUR ANALYSIS
# =====================================================================
st.header("3 — Peak Hour Congestion")

my_hourly = hourly_df[hourly_df["airport_icao"] == selected].copy()

if not my_hourly.empty:
    peak_hour = my_hourly.loc[my_hourly["flights"].idxmax()]
    total_flights = my_hourly["flights"].sum()
    peak_flights = int(peak_hour["flights"])
    peak_pct = round(peak_flights / total_flights * 100, 1)

    col_heat, col_insight = st.columns([3, 2])

    with col_heat:
        fig_hourly = go.Figure()

        # Bar chart for selected airport
        fig_hourly.add_trace(go.Bar(
            x=my_hourly["departure_hour_local"],
            y=my_hourly["flights"],
            marker_color=["#EF553B" if f >= peak_flights * 0.85 else "#636EFA"
                          for f in my_hourly["flights"]],
        ))
        fig_hourly.update_layout(
            template=TEMPLATE,
            xaxis_title="Hour (Local Time)",
            yaxis_title="Total Flights (7 days)",
            xaxis=dict(dtick=1),
            height=350, margin=dict(t=10),
        )
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col_insight:
        # Find quiet hours (below 30% of peak)
        quiet_mask = my_hourly["flights"] < peak_flights * 0.3
        quiet_hours = my_hourly[quiet_mask]["departure_hour_local"].tolist()
        quiet_str = ", ".join(f"{int(h):02d}:00" for h in sorted(quiet_hours)) if quiet_hours else "None"

        # Find congested hours (above 85% of peak)
        busy_mask = my_hourly["flights"] >= peak_flights * 0.85
        busy_hours = my_hourly[busy_mask]["departure_hour_local"].tolist()
        busy_str = ", ".join(f"{int(h):02d}:00" for h in sorted(busy_hours))

        # Operating window
        active = my_hourly[my_hourly["flights"] > peak_flights * 0.3]["departure_hour_local"]
        if not active.empty:
            window_start = f"{int(active.min()):02d}:00"
            window_end = f"{int(active.max()):02d}:00"
        else:
            window_start, window_end = "N/A", "N/A"

        st.markdown("**Peak Hour Analysis**")
        st.markdown(f"""
        | Metric | Value |
        |---|---|
        | Peak hour | **{int(peak_hour['departure_hour_local']):02d}:00 local** |
        | Peak volume | {peak_flights} flights ({peak_pct}% of daily) |
        | Operating window | {window_start} — {window_end} |
        | Congested hours (>85% peak) | {busy_str} |
        | Quiet hours (<30% peak) | {quiet_str} |
        """)

    # Recommendation
    congested_count = len(busy_hours)
    if congested_count >= 5:
        st.warning(
            f"**Congestion risk:** {city_map[selected]} has **{congested_count} hours at >85% of peak capacity**. "
            f"Recommendation: introduce off-peak landing fee incentives to redistribute "
            f"traffic from {busy_str} toward underutilized slots ({quiet_str})."
        )
    elif congested_count <= 2:
        st.success(
            f"**Well-distributed traffic:** Only {congested_count} congested hours. "
            f"Current slot allocation is efficient."
        )

st.divider()

# =====================================================================
# SECTION 4: AIRLINE CONCENTRATION RISK
# =====================================================================
st.header("4 — Airline Concentration Risk")

my_airlines = airline_df[airline_df["airport_icao"] == selected].copy()

if not my_airlines.empty:
    total = my_airlines["flights"].sum()
    my_airlines["share_pct"] = round(my_airlines["flights"] / total * 100, 1)
    top5 = my_airlines.head(5)
    top1_share = top5["share_pct"].iloc[0]
    top3_share = top5["share_pct"].head(3).sum()

    col_pie, col_table = st.columns([2, 3])

    with col_pie:
        # Top 5 + Others
        others = pd.DataFrame({
            "airline_name": ["Others"],
            "alliance": [""],
            "flights": [total - top5["flights"].sum()],
            "share_pct": [round(100 - top5["share_pct"].sum(), 1)],
        })
        pie_data = pd.concat([top5[["airline_name", "flights", "share_pct"]], others], ignore_index=True)

        fig_pie = px.pie(
            pie_data,
            values="flights",
            names="airline_name",
            hole=0.4,
            template=TEMPLATE,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        fig_pie.update_layout(
            height=350, margin=dict(t=10, b=10),
            showlegend=False,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_table:
        st.markdown("**Top Airlines at this Airport**")
        display_airlines = top5[["airline_name", "alliance", "flights", "share_pct"]].rename(
            columns={
                "airline_name": "Airline",
                "alliance": "Alliance",
                "flights": "Flights",
                "share_pct": "Market Share %",
            }
        )
        st.dataframe(display_airlines, use_container_width=True, hide_index=True)

        # HHI concentration index
        hhi = int((my_airlines["share_pct"] ** 2).sum())
        st.caption(f"HHI Concentration Index: **{hhi}** (>2500 = highly concentrated, <1500 = competitive)")

    # Recommendation
    if top1_share > 30:
        st.error(
            f"**High dependency risk:** {top5['airline_name'].iloc[0]} controls **{top1_share}%** "
            f"of traffic at {city_map[selected]}. If this airline reduces service, the airport "
            f"faces significant revenue and connectivity loss. "
            f"Recommendation: actively pursue route development with alternative carriers."
        )
    elif top3_share > 70:
        st.warning(
            f"**Moderate concentration:** Top 3 airlines control **{top3_share:.0f}%** of traffic. "
            f"Diversification through low-cost carrier incentives would reduce risk."
        )
    else:
        st.success(
            f"**Healthy diversification:** No single airline exceeds 30% share, "
            f"and top 3 control {top3_share:.0f}%. Low dependency risk."
        )

st.divider()

# =====================================================================
# SECTION 5: ROUTE CONNECTIVITY
# =====================================================================
st.header("5 — Route Connectivity")

my_routes = routes_df[
    (routes_df["departure_airport_icao"] == selected) |
    (routes_df["arrival_airport_icao"] == selected)
].head(10).copy()

if not my_routes.empty:
    my_routes["route"] = my_routes["departure_city"] + " → " + my_routes["arrival_city"]

    col_routes, col_mix = st.columns([3, 2])

    with col_routes:
        routes_sorted = my_routes.sort_values("total_flights", ascending=True)
        fig_routes = px.bar(
            routes_sorted,
            x="total_flights",
            y="route",
            orientation="h",
            color="avg_duration_min",
            color_continuous_scale="Blues",
            labels={
                "total_flights": "Total Flights",
                "route": "",
                "avg_duration_min": "Avg Duration (min)",
            },
            template=TEMPLATE,
        )
        fig_routes.update_layout(
            height=400, margin=dict(t=10),
            yaxis=dict(dtick=1),
        )
        st.plotly_chart(fig_routes, use_container_width=True)

    with col_mix:
        # Route type breakdown
        my_routes["route_type"] = my_routes["avg_duration_min"].apply(
            lambda m: "Short-haul (<2h)" if m < 120
            else ("Medium-haul (2-6h)" if m < 360 else "Long-haul (6h+)")
        )
        mix = my_routes.groupby("route_type")["total_flights"].sum().reset_index()

        fig_mix = px.pie(
            mix,
            values="total_flights",
            names="route_type",
            color="route_type",
            color_discrete_map={
                "Short-haul (<2h)": "#00CC96",
                "Medium-haul (2-6h)": "#636EFA",
                "Long-haul (6h+)": "#EF553B",
            },
            hole=0.4,
            template=TEMPLATE,
        )
        fig_mix.update_traces(textposition="inside", textinfo="percent+label")
        fig_mix.update_layout(
            height=350, margin=dict(t=10, b=10),
            showlegend=False,
        )
        st.plotly_chart(fig_mix, use_container_width=True)

        # Connectivity summary
        total_routes = len(routes_df[
            (routes_df["departure_airport_icao"] == selected) |
            (routes_df["arrival_airport_icao"] == selected)
        ])
        unique_destinations = len(set(
            routes_df[routes_df["departure_airport_icao"] == selected]["arrival_airport_icao"].tolist() +
            routes_df[routes_df["arrival_airport_icao"] == selected]["departure_airport_icao"].tolist()
        ))
        st.metric("Total Routes", total_routes)
        st.metric("Unique Destinations", unique_destinations)

    # Route recommendation
    short_haul = my_routes[my_routes["route_type"] == "Short-haul (<2h)"]["total_flights"].sum()
    long_haul = my_routes[my_routes["route_type"] == "Long-haul (6h+)"]["total_flights"].sum()
    total_shown = my_routes["total_flights"].sum()

    if total_shown > 0 and short_haul / total_shown > 0.6:
        st.info(
            f"**Domestic/regional hub profile:** {city_map[selected]} is dominated by short-haul "
            f"routes ({round(short_haul/total_shown*100)}% of top route traffic). "
            f"Opportunity: attract long-haul carriers to increase per-passenger revenue "
            f"and airport commercial spend."
        )
    elif total_shown > 0 and long_haul / total_shown > 0.5:
        st.info(
            f"**International hub profile:** {city_map[selected]} skews long-haul "
            f"({round(long_haul/total_shown*100)}% of top route traffic). "
            f"Ensure premium lounge capacity and transit facilities match demand."
        )

# ── Footer ────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Airport Operations Intelligence | "
    "Data: OpenSky Network API | "
    "Pipeline: Airflow → Snowflake → dbt → Streamlit | "
    "Note: Dubai (OMDB) traffic reduced due to regional geopolitical factors (US-Iran conflict)."
)
