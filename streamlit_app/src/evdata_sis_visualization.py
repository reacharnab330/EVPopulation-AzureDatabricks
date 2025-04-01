# streamlit_app.py
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException
import plotly.express as px
import plotly.graph_objects as go

# Get the active Snowpark session
session = get_active_session()

# Styling for the app
st.set_page_config(page_title="EV Insights", layout="wide")
st.markdown("""
    <style>
    .main-title { font-size: 36px; color: #2E86C1; text-align: center; }
    .subheader { font-size: 20px; color: #2874A6; }
    .caption { font-size: 12px; color: #566573; }
    .insight-box { border: 2px solid #2E86C1; padding: 10px; border-radius: 10px; margin-bottom: 10px; }
    </style>
""", unsafe_allow_html=True)

# Title
st.markdown('<p class="main-title">Electric Vehicle Population Insights</p>', unsafe_allow_html=True)

# Helper function to debug column names
def debug_columns(df, label):
    st.write(f"Columns in {label}: {df.columns.tolist()}")



# Create a 2x2 layout using columns
col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

# Insight 1: Most Efficient Car Make (Top Left)
with col1:
    st.markdown('<div class="insight-box">', unsafe_allow_html=True)
    st.markdown('<p class="subheader">Top 5 Most Efficient Car Makes by Avg Range</p>', unsafe_allow_html=True)
    query_efficiency = """
        SELECT "MAKE" AS "make", AVG("ELECTRIC_RANGE") AS "avg_range"
        FROM "EVDATA_DB"."PUBLIC"."EV_POPULATION"
        WHERE "ELECTRIC_RANGE" > 0
        GROUP BY "MAKE"
        ORDER BY "avg_range" DESC
        LIMIT 5
    """
    try:
        efficiency_df = session.sql(query_efficiency).to_pandas()
        efficiency_df.columns = efficiency_df.columns.str.lower()
        #debug_columns(efficiency_df, "Efficiency DataFrame")
        
        # Plotly Bar Chart
        fig1 = px.bar(
            efficiency_df,
            x="make",
            y="avg_range",
            color="make",
            labels={"make": "Car Make", "avg_range": "Avg Range (miles)"},
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig1.update_layout(showlegend=False, plot_bgcolor="white", font=dict(size=12), height=300)
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown('<p class="caption">Hover for details!</p>', unsafe_allow_html=True)
    except SnowparkSQLException as e:
        st.error(f"Error in Efficiency Query: {str(e)}")
    st.markdown('</div>', unsafe_allow_html=True)

# Insight 2: Popular PHEV Models (Top Right)
with col2:
    st.markdown('<div class="insight-box">', unsafe_allow_html=True)
    st.markdown('<p class="subheader">Top 5 Most Popular PHEV Models</p>', unsafe_allow_html=True)
    query_phev = """
        SELECT "MODEL" AS "model", COUNT(*) AS "count"
        FROM "EVDATA_DB"."PUBLIC"."EV_POPULATION"
        WHERE "ELECTRIC_VEHICLE_TYPE" = 'Plug-in Hybrid Electric Vehicle (PHEV)'
        GROUP BY "MODEL"
        ORDER BY "count" DESC
        LIMIT 5
    """
    try:
        phev_df = session.sql(query_phev).to_pandas()
        phev_df.columns = phev_df.columns.str.lower()
        #debug_columns(phev_df, "PHEV DataFrame")
        
        # Plotly Pie Chart
        fig2 = px.pie(
            phev_df,
            names="model",
            values="count",
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig2.update_traces(textinfo="percent+label", pull=[0.1, 0, 0])
        fig2.update_layout(font=dict(size=12), height=300)
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('<p class="caption">Interactive pie chart.</p>', unsafe_allow_html=True)
    except SnowparkSQLException as e:
        st.error(f"Error in PHEV Query: {str(e)}")
    st.markdown('</div>', unsafe_allow_html=True)

# Insight 3: Recommended Make/Model (Bottom Left)
with col3:
    st.markdown('<div class="insight-box">', unsafe_allow_html=True)
    st.markdown('<p class="subheader">Recommended Car Make & Model - Avg Range & Popularity</p>', unsafe_allow_html=True)
    query_recommendation = """
        SELECT "MAKE" AS "make", "MODEL" AS "model", AVG("ELECTRIC_RANGE") AS "avg_range", COUNT(*) AS "popularity"
        FROM "EVDATA_DB"."PUBLIC"."EV_POPULATION"
        WHERE "ELECTRIC_RANGE" > 0
        GROUP BY "MAKE", "MODEL"
        HAVING COUNT(*) > 10
        ORDER BY "avg_range" DESC, "popularity" DESC
        LIMIT 5
    """
    try:
        recommendation_df = session.sql(query_recommendation).to_pandas()
        recommendation_df.columns = recommendation_df.columns.str.lower()
        #debug_columns(recommendation_df, "Recommendation DataFrame")
        
        # Styled Table
        st.dataframe(
            recommendation_df.style.format({"avg_range": "{:.1f}", "popularity": "{:,.0f}"}),
            use_container_width=True,
            height=225
        )
        st.markdown('<p class="caption">Top pick by range & adoption.</p>', unsafe_allow_html=True)
    except SnowparkSQLException as e:
        st.error(f"Error in Recommendation Query: {str(e)}")
    st.markdown('</div>', unsafe_allow_html=True)

# Insight 4: EV Make Preference by City (Bottom Right)
with col4:
    st.markdown('<div class="insight-box">', unsafe_allow_html=True)
    st.markdown('<p class="subheader">EV Make Preference in Top Cities</p>', unsafe_allow_html=True)
    query_city = """
                select 
                    final_set.city as "city" ,
                    final_set.make  as "make",
                    final_set.make_ev_count as "count"
                from
                    (select 
                        top_ev_city_make.city, 
                        top_ev_city_make.total_ev_count,
                        top_ev_city_make.make, 
                        top_ev_city_make.make_ev_count,
                        rank() over (partition by top_ev_city_make.city order by top_ev_city_make.make_ev_count desc ) as ev_make_rank_in_city
                    from
                        (select ev2.city, ev2.make, ev1.total_ev_count, count(1) as make_ev_count 
                        from "EVDATA_DB"."PUBLIC"."EV_POPULATION" as  ev2
                        inner join (select city, count(1) as total_ev_count 
                                    from "EVDATA_DB"."PUBLIC"."EV_POPULATION"
                                    group by city 
                                    order by total_ev_count desc limit 5) as ev1
                        on ev1.city = ev2.city
                        group by  ev2.city, ev2.make, ev1.total_ev_count) as top_ev_city_make) as final_set
                where 
                        final_set.ev_make_rank_in_city <= 5
                order by 
                    final_set.city
             """
    try:
        city_df = session.sql(query_city).to_pandas()
        city_df.columns = city_df.columns.str.lower()
        #debug_columns(city_df, "City DataFrame")
        
        # Plotly Stacked Bar Chart
        fig4 = px.bar(
            city_df,
            x="city",
            y="count",
            color="make",
            labels={"city": "City", "count": "Number of EVs", "make": "Make"},
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig4.update_layout(barmode="stack", plot_bgcolor="white", font=dict(size=12), height=300)
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown('<p class="caption">Stacked bars by city.</p>', unsafe_allow_html=True)
    except SnowparkSQLException as e:
        st.error(f"Error in City Query: {str(e)}")
    st.markdown('</div>', unsafe_allow_html=True)