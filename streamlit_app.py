import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import logging
import json
import os
 
# Set up logging
logging.basicConfig(level=logging.ERROR, filename="error.log")
 
# Streamlit setup
st.set_page_config(page_title="Buffet Sale Performance Report", layout="wide")
 
# Data schemas
DAILY_SALES_AGGREGATED_TABLE_ID = "golden-passkey-439311-c8.f2.Daily_Sales_Aggregated"
MONTH_SALES_SUMMARY_TABLE_ID = "golden-passkey-439311-c8.f2.month_sales_summary"
 
DAILY_SALES_AGGREGATED_SCHEMA = [
    {"table_id": DAILY_SALES_AGGREGATED_TABLE_ID},
    {"field_name": "ETL_Date", "type": "TIMESTAMP", "description": "The date and time when the data was extracted, transformed, and loaded (ETL)."},
    {"field_name": "Branch_ID", "type": "STRING", "description": "Unique identifier for each branch."},
    {"field_name": "Sales_Date", "type": "DATE", "description": "The date associated with the sales data."},
    {"field_name": "Total_Daily_Sales", "type": "NUMERIC(18, 2)", "description": "Total daily sales amount for the branch."},
    {"field_name": "Daily_Target", "type": "NUMERIC(18, 2)", "description": "The sales target set for the branch for the respective date."},
]
 
MONTH_SALES_SUMMARY_SCHEMA = [
    {"table_id": MONTH_SALES_SUMMARY_TABLE_ID},
    {"field_name": "ETL_Date", "type": "TIMESTAMP", "description": "Date and time of ETL process."},
    {"field_name": "Year", "type": "INTEGER", "description": "Year of the sales data."},
    {"field_name": "Year_Month", "type": "STRING", "description": "Year and month in 'YYYY-MM' format."},
    {"field_name": "Month_Name", "type": "STRING", "description": "Month name (e.g., January)."},
    {"field_name": "Branch_ID", "type": "STRING", "description": "Branch identifier."},
    {"field_name": "Branch_Name", "type": "STRING", "description": "Branch name."},
    {"field_name": "Total_Monthly_Sales", "type": "NUMERIC", "description": "Total sales for the branch in the month."},
    {"field_name": "Monthly_Target", "type": "NUMERIC", "description": "Sales target for the branch for the month."},
    {"field_name": "Number_Of_Customer", "type": "INTEGER", "description": "Number of customers served in the month."},
]
 
# Sidebar setup
logo_url = "https://cdn-icons-png.flaticon.com/512/8339/8339330.png"
with st.sidebar:
    st.markdown(
        f"""
        <div style="display: flex; align-items: center;">
            <img src="{logo_url}" alt="Logo" style="width: 30px; margin-right: 10px;">
            <h3 style="margin: 0;">Buffet Sale Performance</h3>
        </div>
        """,
        unsafe_allow_html=True,
    )
    gemini_api_key = st.text_input("Gemini API Key 🔑", placeholder="Enter your API Key...", type="password")
    page = st.selectbox("Navigate to:", ["Chat", "Dashboard"])
 
# Configure Gemini API
model = None
agent_01 = None
agent_02 = None
if gemini_api_key:
    try:
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel("gemini-pro")
        agent_01 = genai.GenerativeModel("gemini-pro")
        agent_02 = genai.GenerativeModel("gemini-pro")
        st.sidebar.success("Gemini API Key configured successfully.")
    except Exception as e:
        st.sidebar.error(f"Error configuring Gemini API: {e}")
        logging.error(f"Gemini API Error: {e}")
else:
    st.sidebar.warning("Please provide a valid Gemini API Key.")
 
# BigQuery setup
try:
    service_account_info = st.secrets["gcp_service_account"]
    client = bigquery.Client.from_service_account_info(service_account_info)
except Exception as e:
    st.error(f"Error setting up BigQuery: {e}")
    logging.error(f"BigQuery Setup Error: {e}")
    st.stop()
 
# Initialize session state
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
 
if "sales_summary_displayed" not in st.session_state:
    st.session_state.sales_summary_displayed = False
 
# Query Guide
query_guide = {
    "Which branch had the highest sales in February?": {
        "query": """
            SELECT
              f2.Branch_ID AS Branch_ID,
              f2.Branch_Name AS Branch_Name,
              SUM(f2.Total_Monthly_Sales) AS Sales
            FROM
              `golden-passkey-439311-c8.f2.month_sales_summary` AS f2
            WHERE
              f2.Year_Month = '2024-02'
            GROUP BY
              f2.Branch_ID, f2.Branch_Name
            ORDER BY
              Sales DESC
            LIMIT 1;
        """,
        "response_template": "The branch with the highest sales in February is {Branch_Name} (ID: {Branch_ID}) with total sales of {Sales} baht."
    }
}
 
# Display Sales Summary
def display_sales_summary(client: bigquery.Client):
    try:
        query = """
            SELECT f2.Branch_ID,
                   f2.Branch_Name,
                   f2.Total_Daily_Sales
            FROM `golden-passkey-439311-c8.f2.daily_sales_summary` as f2
            WHERE DATE(f2.Sales_date) = (
                SELECT MAX(DATE(Sales_date))
                FROM `golden-passkey-439311-c8.f2.daily_sales_summary`
            );
        """
        query_job = client.query(query)
        results = query_job.result()
 
        formatted_summary = [
            f"Branch {row['Branch_ID']} {row['Branch_Name']} total sale is {row['Total_Daily_Sales']} baht"
            for row in results
        ]
        return "\n".join(formatted_summary)
    except Exception as e:
        st.error(f"Error fetching sales summary: {e}")
        logging.error(f"Error fetching sales summary: {e}")
        return None
 
# Function to handle general questions using Agent 02
def handle_general_questions(user_input):
    if agent_02:
        try:
            # Prompt Agent 02 with the general info context
            general_prompt = f"""
            You are an assistant for a shabu buffet restaurant. Use the following information to answer the user's question:
 
            {general_info}
 
            User's question:
            "{user_input}"
 
            If you don't know the answer, generate a random shabu buffet-related response.
            """
            response = agent_02.generate_content(general_prompt)
            if response.text.strip():
                return response.text.strip()
            else:
                # Fallback to random response if no specific answer is found
                return random.choice(fallback_responses)
        except Exception as e:
            logging.error(f"Error in Agent 02 response generation: {e}")
            return random.choice(fallback_responses)
    else:
        return "Agent 02 is not configured. Please provide a valid Gemini API Key."
 
# Updated categorize_task Function
def categorize_task(user_input):
    # Keywords to identify query-related tasks
    query_related_keywords = ["sales", "target", "growth", "customer", "branch"]
 
    # Check if the input matches query-related tasks
    if any(keyword in user_input.lower() for keyword in query_related_keywords):
        if agent_01:
            try:
                # Match user input to predefined queries in query_guide
                for question, details in query_guide.items():
                    if question.lower() in user_input.lower():
                        query = details["query"]
                        response_template = details["response_template"]
 
                        # Execute the SQL query
                        query_job = client.query(query)
                        results = query_job.result()
                        result_dict = [dict(row) for row in results]
 
                        # Format the response using the template
                        if result_dict:
                            return response_template.format(
                                result="\n".join(
                                    ", ".join(f"{key}: {value}" for key, value in row.items()) for row in result_dict
                                ),
                                **result_dict[0]
                            )
                        else:
                            return "No results found for your query."
 
                # Generate a query dynamically if no match is found in the guide
                dynamic_query_prompt = f"""
                Based on the schema and query guide, generate a SQL query to address the user input:
 
                **Schema Definitions:**
                DAILY_SALES_AGGREGATED_SCHEMA:
                {json.dumps(DAILY_SALES_AGGREGATED_SCHEMA, indent=2)}
 
                MONTH_SALES_SUMMARY_SCHEMA:
                {json.dumps(MONTH_SALES_SUMMARY_SCHEMA, indent=2)}
 
                **User Input:**
                "{user_input}"
 
                Respond with a valid SQL query without explaining it.
                """
                response = agent_01.generate_content(dynamic_query_prompt)
                dynamic_query = response.text.strip()
 
                # Execute the dynamically generated query
                query_job = client.query(dynamic_query)
                results = query_job.result()
                result_dict = [dict(row) for row in results]
 
                if result_dict:
                    return "\n".join(
                        ", ".join(f"{key}: {value}" for key, value in row.items()) for row in result_dict
                    )
                else:
                    return "No results found for your query."
 
            except Exception as e:
                logging.error(f"Error in Agent 01 task categorization: {e}")
                return "Sorry, I couldn't process your query. Please try again later."
        else:
            return "Agent 01 is not configured. Please provide a valid Gemini API Key."
    else:
        # Handle general questions using Agent 02
        return handle_general_questions(user_input)
 
query_guide = {
    "Percentage growth comparison for this year vs. last year": {
        "query": """
            SELECT
                f2_year_sales.Year AS Year,
                f2_year_sales.Branch_ID AS Branch_ID,
                f2_year_sales.Branch_Name AS Branch_Name,
                f2_year_sales.Growth_year AS Growth_year
            FROM
                `golden-passkey-439311-c8.f2.year_sales_summary` AS f2_year_sales
            WHERE
                f2_year_sales.Year = 2024
            GROUP BY
                f2_year_sales.Year, f2_year_sales.Branch_ID, f2_year_sales.Branch_Name, f2_year_sales.Growth_year
            ORDER BY
                Growth_year DESC;
        """,
        "response_template": "Percentage growth comparison for 2024:\n{result}"
    },
    "What is the % growth for each branch?": {
        "query": """
            SELECT
                f2_month_sales.Branch_ID AS Branch_ID,
                f2_month_sales.Branch_Name AS Branch_Name,
                f2_month_sales.Growth_year AS Percentage_Growth_year
            FROM
                `golden-passkey-439311-c8.f2.month_sales_summary` AS f2_month_sales
            WHERE f2_month_sales.Year_Month = '2024-02'
            GROUP BY
                f2_month_sales.Branch_ID, f2_month_sales.Branch_Name, f2_month_sales.Growth_year
            ORDER BY
                Percentage_Growth_year DESC;
        """,
        "response_template": "Here is the percentage growth for each branch in February 2024:\n{result}"
    },
    "Customer count changes this month": {
        "query": """
            WITH ranked_data AS (
                SELECT
                    f2_month_sales.Year_Month AS Year_Month,
                    f2_month_sales.Branch_ID AS Branch_ID,
                    f2_month_sales.Branch_Name AS Branch_Name,
                    f2_month_sales.Number_Of_customer AS Number_Of_customer,
                    LAG(f2_month_sales.Number_Of_customer) OVER (
                        PARTITION BY f2_month_sales.Branch_ID
                        ORDER BY f2_month_sales.Year_Month
                    ) AS Last_Month_Customers
                FROM
                    `golden-passkey-439311-c8.f2.month_sales_summary` AS f2_month_sales
            )
            SELECT
                Branch_ID,
                Branch_Name,
                Year_Month,
                Number_Of_customer,
                Last_Month_Customers,
                Number_Of_customer - Last_Month_Customers AS Difference
            FROM ranked_data
            WHERE Year_Month = '2024-02'
            ORDER BY
                Year_Month DESC;
        """,
        "response_template": "Here is the customer count change for February 2024:\n{result}"
    },
    "How much has the customer count increased this month?": {
        "query": """
            SELECT
                daily_sales.Sales_Date AS Sales_Date,
                daily_sales.Branch_ID AS Branch_ID,
                daily_sales.Branch_Name AS Branch_Name,
                daily_sales.Total_Daily_Sales AS Total_Daily_Sales
            FROM `golden-passkey-439311-c8.f2.daily_sales_summary` AS daily_sales
            WHERE DATE(daily_sales.Sales_Date) BETWEEN '2024-02-01' AND '2024-02-29'
            GROUP BY daily_sales.Branch_ID, daily_sales.Branch_Name, daily_sales.Sales_Date, daily_sales.Total_Daily_Sales;
        """,
        "response_template": "Customer count for February 2024 has increased as follows:\n{result}"
    },
    "What are today's sales for this branch?": {
        "query": """
            SELECT
                Branch_ID,
                Branch_Name,
                SUM(Total_Daily_Sales) AS Today_Sales
            FROM `golden-passkey-439311-c8.f2.daily_sales_summary`
            WHERE DATE(Sales_Date) = '2024-02-29'
            GROUP BY Branch_ID, Branch_Name;
        """,
        "response_template": "Today's sales for February 29, 2024, are:\n{result}"
    },
    "End-of-month sales target overview": {
        "query": """
            SELECT
                Branch_ID,
                Branch_Name,
                Total_Monthly_Sales AS Actual_Sales,
                Monthly_Target AS Target_Sales,
                Total_Monthly_Sales - Monthly_Target AS Diff
            FROM `golden-passkey-439311-c8.f2.month_sales_summary`
            WHERE Year_Month = '2024-02'
            ORDER BY Diff ASC
            LIMIT 1;
        """,
        "response_template": "End-of-month sales target overview for February 2024:\n{result}"
    },
    "Will the overall sales reach the set target by the end of this month?": {
        "query": """
            SELECT
                Year_Month,
                Branch_ID,
                Branch_Name,
                Total_Monthly_Sales AS This_Month_Sales,
                Last_Month_Sales,
                Total_Monthly_Sales - Last_Month_Sales AS Diff
            FROM `golden-passkey-439311-c8.f2.month_sales_summary`
            WHERE Year_Month = '2024-02'
            ORDER BY Diff ASC
            LIMIT 1;
        """,
        "response_template": "Sales progress for February 2024:\n{result}"
    },
    "Which branch is currently the furthest from its target?": {
        "query": """
            SELECT
                Branch_ID,
                Branch_Name,
                Total_Monthly_Sales AS Actual_Sales,
                Monthly_Target AS Target_Sales,
                Total_Monthly_Sales - Monthly_Target AS Diff
            FROM `golden-passkey-439311-c8.f2.month_sales_summary`
            WHERE Year_Month = '2024-02'
            ORDER BY Diff ASC
            LIMIT 1;
        """,
        "response_template": "The branch furthest from its target in February 2024 is {Branch_Name} (ID: {Branch_ID}) with actual sales of {Actual_Sales} baht, a target of {Target_Sales} baht, and a difference of {Diff} baht."
    }
}
 
# General info context
general_info = """
**About Us**  
We are a small buffet restaurant steadily growing alongside the delicious flavors we serve.
We invite you to savor the taste and variety of dishes we have carefully prepared for you to enjoy during our 2-hour all-you-can-eat experience.  
 
**Price**: Affordable and pocket-friendly, just 299 THB per person.  
**Products**: In addition to our delightful and diverse shabu buffet, we also offer bubble tea, our signature secret broths, and exclusive sauces.  
**Established**: Providing quality shabu dining since 2023.  
**Branches**: 5 convenient locations.  
**Operating Hours**: Open daily: 11:00 AM - 10:00 PM.
"""
 
fallback_responses = [
    "Shabu is love, shabu is life! Let us know how we can make your dining experience better.",
    "Did you know our secret broth recipe has been passed down for generations?",
    "You’ll love our exclusive sauces! What else would you like to know?",
    "Bubble tea and shabu – a match made in heaven. Come visit us soon!",
    "Our 299 THB buffet is waiting for you! What other questions do you have?"
]
 
# Chat Page Function
def display_chat_page():
    # เพิ่ม CSS สำหรับปรับแต่งส่วนหัว
    st.markdown(
        """
        <style>
        .main-header {
            font-size: 48px;
            font-weight: bold;
            color: darkorange;
            text-align: center;
            margin-bottom: 10px;
        }
        .sub-header {
            font-size: 34px;
            font-weight: bold;
            margin-top: 20px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
 
    # เพิ่มชื่อหัวข้อ "Buffet Sale"
    st.markdown('<div class="main-header">🥘 Buffet Sale Performance 📈</div>', unsafe_allow_html=True)
 
    # เพิ่มหัวข้อย่อย "Chat with Sale Assistance to Get Insight"
    st.markdown('<div class="sub-header">🤖💬 Chat with Sale Assistance to Get Insight!</div>', unsafe_allow_html=True)
 
 
    # Check for API key input and display sales summary if not already displayed
    if gemini_api_key and not st.session_state.sales_summary_displayed:
        summary = display_sales_summary(client)
        if summary:
            st.chat_message("assistant").markdown(f"### Daily Sales Summary:\n\n{summary}")
            # Append the sales summary to chat history
            st.session_state.chat_history.append(("assistant", f"### Daily Sales Summary:\n\n{summary}"))
        else:
            no_summary_message = "No sales summary available at the moment."
            st.chat_message("assistant").markdown(no_summary_message)
            # Append the no summary message to chat history
            st.session_state.chat_history.append(("assistant", no_summary_message))
        st.session_state.sales_summary_displayed = True
 
    # Display previous chat history
    for role, message in st.session_state.chat_history:
        st.chat_message(role).markdown(message)
 
    # Capture user input
    if user_input := st.chat_input("Type your question here..."):
        st.session_state.chat_history.append(("user", user_input))
        st.chat_message("user").markdown(user_input)
 
        # Use `categorize_task` to respond
        response = categorize_task(user_input)
        agent_role = "agent_01" if any(
            keyword in user_input.lower() for keyword in ["sales", "target", "growth", "customer", "branch"]
        ) else "agent_02"
        st.session_state.chat_history.append((agent_role, response))
        st.chat_message(agent_role).markdown(response)
 
 
 
# Dashboard Page Function
def display_dashboard_page():
    # เพิ่ม CSS สำหรับปรับแต่งส่วนหัว
    st.markdown(
        """
        <style>
        .dashboard-header {
            font-size: 40px;
            font-weight: bold;
            color: darkorange;
            text-align: center;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
 
    # เพิ่มส่วนหัวที่มีสไตล์
    st.markdown('<div class="dashboard-header">🥘 Buffet Sale Performance Dashboard 📊</div>', unsafe_allow_html=True)
 
    # แสดง Power BI Dashboard
    dashboard_url = "https://app.powerbi.com/view?r=eyJrIjoiNjlhNmFjMGUtZGY2Zi00MDEyLWE4NDItODNkOTkzN2UwYTU4IiwidCI6ImRiNWRlZjZiLThmZDgtNGEzZS05MWRjLThkYjI1MDFhNjgyMiIsImMiOjEwfQ%3D%3D"
    st.markdown(
        f"""
        <iframe title="🥘 Buffet Sale Performance Dashboard 📊" width="100%" height="800" src="{dashboard_url}" frameborder="0" allowfullscreen="true"></iframe>
        """,
        unsafe_allow_html=True,
    )
 
 
# Display selected page
if page == "Chat":
    display_chat_page()
elif page == "Dashboard":
    display_dashboard_page()
 
# Footer
st.markdown("---")
st.write("Buffet Sale Performance Report | Powered by BigQuery, Gemini API, and Power BI")