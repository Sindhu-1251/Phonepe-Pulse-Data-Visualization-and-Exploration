import os
import json
import pandas as pd
import pymysql
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu

# Set page configuration
phn = Image.open(r"C:\Users\sindh\OneDrive\Desktop\Pro_img\phonepe-logo-icon.png")
st.set_page_config(page_title='PhonePe Pulse', page_icon=phn, layout='wide')

# Continue with the rest of your script
st.title(':violet[ PhonePe Pulse Data Visualization And Exploration ]')

SELECT = option_menu(
        menu_title = None,
        options = ["Home","Search","Insights"],
        icons =["house","toggles","search","toggles","bar-chart"],
        default_index=2,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "white","size":"cover"},
            "icon": {"color": "black", "font-size": "20px"},
            "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#6F36AD"},
            "nav-link-selected": {"background-color": "#6F36AD"}
        }

    )

if SELECT == "Home":
    st.subheader(
        "PhonePe  is an Indian digital payments and financial technology company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016. It is owned by Flipkart, a subsidiary of Walmart.")
    st.image(r"C:\Users\sindh\OneDrive\Desktop\Pro_img\scan.gif")

if SELECT == "Search":
    Topic = ["","Aggregated Transaction", "Aggregated User", "Map Transaction", "Map User", "Top Transaction", "Top User"]
    choice_topic = st.selectbox("Search by",Topic)

    if choice_topic == "Aggregated Transaction":
        def filter_data(state_year_quarter_data, selected_year, selected_state, selected_quarter):
            if selected_year != '--Select--' and selected_state != '--Select--' and selected_quarter != '--Select--':
                return state_year_quarter_data[(state_year_quarter_data['Years'] == selected_year) & 
                                                    (state_year_quarter_data['States'] == selected_state) & 
                                                    (state_year_quarter_data['Quarter'] == selected_quarter)]
            elif selected_year != '--Select--' and selected_state != '--Select--':
                return state_year_quarter_data[(state_year_quarter_data['Years'] == selected_year) & 
                                                    (state_year_quarter_data['States'] == selected_state)]
            elif selected_year != '--Select--' and selected_quarter != '--Select--':
                return state_year_quarter_data[(state_year_quarter_data['Years'] == selected_year) & 
                                                    (state_year_quarter_data['Quarter'] == selected_quarter)]
            elif selected_state != '--Select--' and selected_quarter != '--Select--':
                return state_year_quarter_data[(state_year_quarter_data['States'] == selected_state) & 
                                                    (state_year_quarter_data['Quarter'] == selected_quarter)]
            elif selected_year != '--Select--':
                return state_year_quarter_data[state_year_quarter_data['Years'] == selected_year]
            elif selected_state != '--Select--':
                return state_year_quarter_data[state_year_quarter_data['States'] == selected_state]
            elif selected_quarter != '--Select--':
                    return state_year_quarter_data[state_year_quarter_data['Quarter'] == selected_quarter]
            else:
                    return state_year_quarter_data

        # Dataframe of aggregated Transactions
        path_1 = "C:/Users/sindh/OneDrive/Desktop/Projects/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
        aggregated_transaction_list = os.listdir(path_1)

        col_1 = {"States": [], "Years": [], "Quarter": [],
                "Transaction_Name": [], "Transaction_Count": [], "Transaction_Amount": []}

        for a in aggregated_transaction_list:  # state
            current_state = path_1 + a + "/"
            aggregated_year_list = os.listdir(current_state)

            for b in aggregated_year_list:  # year
                current_year = current_state + b + "/"
                aggregated_file_list = os.listdir(current_year)

                for c in aggregated_file_list:
                    current_file = current_year + c
                    data = open(current_file, "r")

                    s = json.load(data)
                    for i in s["data"]["transactionData"]:
                        name = i["name"]
                        count = i["paymentInstruments"][0]["count"]
                        amount = i["paymentInstruments"][0]["amount"]

                        col_1["Transaction_Name"].append(name)
                        col_1["Transaction_Count"].append(count)
                        col_1["Transaction_Amount"].append(amount)
                        col_1["States"].append(a)
                        col_1["Years"].append(b)
                        col_1["Quarter"].append(int(c.strip(".json")))
        aggregated_transactions_df = pd.DataFrame(col_1)

        aggregated_transactions_df["States"] = aggregated_transactions_df["States"].str.replace("andaman-&-nicobar-islands", "Andaman-&-Nicobar-Islands")
        aggregated_transactions_df["States"] = aggregated_transactions_df["States"].str.replace("-", " ")
        aggregated_transactions_df["States"] = aggregated_transactions_df["States"].str.title()
        aggregated_transactions_df["States"] = aggregated_transactions_df["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Connect to MySQL database
        db = pymysql.connect(host="localhost", user="root", password="123", port=3306)
        cursor = db.cursor()

        database_name = "Phonepe"
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
        cursor.execute(f'USE {database_name}')

        # Create table for aggregated transactions
        cr_query1 = '''CREATE TABLE IF NOT EXISTS aggregated_transaction(
                        States VARCHAR(255),
                        Years INT,
                        Quarter INT,
                        Transaction_Name VARCHAR(255),
                        Transaction_Count BIGINT,
                        Transaction_Amount BIGINT)'''
        cursor.execute(cr_query1)
        db.commit()

        # Insert data into the table
        in_query1 = '''INSERT INTO aggregated_transaction(
                        States, Years, Quarter, Transaction_Name, Transaction_Count, Transaction_Amount)
                        VALUES (%s, %s, %s, %s, %s, %s)'''
        data = aggregated_transactions_df.values.tolist()
        cursor.executemany(in_query1, data)
        db.commit()

        # Aggregate data by state, years, and quarter
        state_year_quarter_data = aggregated_transactions_df.groupby(['States', 'Years', 'Quarter']).agg({'Transaction_Count': 'sum', 'Transaction_Amount': 'sum'}).reset_index()
        
        # Display select boxes for years, states, and quarters at the top
        selected_year = st.selectbox('Select Year', ['--Select--'] + sorted(state_year_quarter_data['Years'].unique()))
        selected_state = st.selectbox('Select State', ['--Select--'] + sorted(state_year_quarter_data['States'].unique()))
        quarters = ['--Select--', 1, 2, 3, 4]
        selected_quarter = st.selectbox('Select Quarter', quarters, index=0)

        # Filter data by selected year, state, and quarter
        filtered_data = filter_data(state_year_quarter_data, selected_year, selected_state, selected_quarter)

        # Display whole India map by default
        #show_whole_map = st.checkbox('Show whole India map', False)

        # Now, creating the India map using Plotly Express
        india_map_geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Create vertical choropleth map for Transaction Count
        fig_count = px.choropleth(
            filtered_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Count',
            color_continuous_scale='Reds',
            title='Transaction Count by State in India'
        )
        fig_count.update_geos(fitbounds="locations", visible=False)

        # Create vertical choropleth map for Transaction Amount with blue colors
        fig_amount = px.choropleth(
            filtered_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Amount',
            color_continuous_scale='Blues',
            title='Transaction Amount by State in India'
        )
        fig_amount.update_geos(fitbounds="locations", visible=False)

        # Create pie chart for Transaction Count
        fig_pie_count = px.pie(
            filtered_data,
            values='Transaction_Count',
            names='States',
            title=f'Transaction Count by State in India'
        )
        fig_pie_count.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_count.update_layout(height=600)

        # Create pie chart for Transaction Amount
        fig_pie_amount = px.pie(
            filtered_data,
            values='Transaction_Amount',
            names='States',
            title=f'Transaction Amount by State in India'
        )
        fig_pie_amount.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_amount.update_layout(height=600)

        # Display maps and pie charts based on radio button selection
        chart_type = st.radio('Select Chart Type', ['Map for Transaction Count', 'Map for Transaction Amount', 'Pie Chart for Transaction Count', 'Pie Chart for Transaction Amount'])

        # Display maps based on radio button selection
        if chart_type == 'Map for Transaction Count':
            st.plotly_chart(fig_count, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_data)

        elif chart_type == 'Map for Transaction Amount':
            st.plotly_chart(fig_amount, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_data)

        elif chart_type == 'Pie Chart for Transaction Count':
            st.plotly_chart(fig_pie_count)

        else:  # 'Pie Chart for Transaction Amount'
            st.plotly_chart(fig_pie_amount)

    if choice_topic == "Aggregated User":
        def filter_user_data(selected_year, selected_state, selected_quarter):
            if selected_year != '--Select--' and selected_state != '--Select--' and selected_quarter != '--Select--':
                return aggregated_user_df[(aggregated_user_df['Years'] == selected_year) & 
                                        (aggregated_user_df['States'] == selected_state) & 
                                        (aggregated_user_df['Quarter'] == selected_quarter)]
            elif selected_year != '--Select--' and selected_state != '--Select--':
                return aggregated_user_df[(aggregated_user_df['Years'] == selected_year) & 
                                        (aggregated_user_df['States'] == selected_state)]
            elif selected_year != '--Select--' and selected_quarter != '--Select--':
                return aggregated_user_df[(aggregated_user_df['Years'] == selected_year) & 
                                        (aggregated_user_df['Quarter'] == selected_quarter)]
            elif selected_state != '--Select--' and selected_quarter != '--Select--':
                return aggregated_user_df[(aggregated_user_df['States'] == selected_state) & 
                                        (aggregated_user_df['Quarter'] == selected_quarter)]
            elif selected_year != '--Select--':
                return aggregated_user_df[aggregated_user_df['Years'] == selected_year]
            elif selected_state != '--Select--':
                return aggregated_user_df[aggregated_user_df['States'] == selected_state]
            elif selected_quarter != '--Select--':
                return aggregated_user_df[aggregated_user_df['Quarter'] == selected_quarter]
            else:
                return aggregated_user_df

        # Dataframe of Aggregated User
        path_2 ="C:/Users/sindh/OneDrive/Desktop/Projects/Phonepe/pulse/data/aggregated/user/country/india/state/"
        aggregated_user_list = os.listdir(path_2)

        col_2 = {"States": [], "Years": [], "Quarter": [], "Brands": [], "Transaction_Count": [], "Percentage": []}

        for a in aggregated_user_list:  # state
            current_state = path_2 + a + "/"
            aggregated_year_list = os.listdir(current_state)

            for b in aggregated_year_list:  # year
                current_year = current_state + b + "/"
                aggregated_file_list = os.listdir(current_year)

                for c in aggregated_file_list:
                    current_file = current_year + c
                    data = open(current_file, "r")
                    try:
                        r = json.load(data)
                        for i in r["data"]["usersByDevice"]:
                            brand = i["brand"]
                            count = i["count"]
                            percentage = i["percentage"]
                            col_2["Brands"].append(brand)
                            col_2["Transaction_Count"].append(count)
                            col_2["Percentage"].append(percentage)
                            col_2["States"].append(a)
                            col_2["Years"].append(b)
                            col_2["Quarter"].append(int(c.strip(".json")))
                    except:
                        pass
        aggregated_user_df = pd.DataFrame(col_2)

        aggregated_user_df["States"] = aggregated_user_df["States"].str.replace("andaman-&-nicobar-islands", "Andaman-&-Nicobar-Islands")
        aggregated_user_df["States"] = aggregated_user_df["States"].str.replace("-", " ")
        aggregated_user_df["States"] = aggregated_user_df["States"].str.title()
        aggregated_user_df["States"] = aggregated_user_df["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Connect to MySQL database
        db = pymysql.connect(host="localhost", user="root", password="123", port=3306, database="Phonepe")
        cursor = db.cursor()

        # Table for Aggreagated User
        cr_query2 = '''CREATE TABLE IF NOT EXISTS aggregated_user(States VARCHAR(255),
                        Years INT,
                        Quarters INT,
                        Brands VARCHAR(255),
                        Transaction_Count BIGINT,
                        Percentage BIGINT)'''
        cursor.execute(cr_query2)
        db.commit()

        # Insert data into the table
        in_query2 = '''INSERT INTO aggregated_user(States, Years, Quarters, 
                    Brands, Transaction_Count, Percentage)
                    
                    VALUES (%s, %s, %s, %s, %s, %s)'''
        data = aggregated_user_df.values.tolist()
        cursor.executemany(in_query2, data)
        db.commit()

        # Display select boxes for years, states, and quarters for user
        selected_year_user = st.selectbox('Select Year for User', ['--Select--'] + sorted(aggregated_user_df['Years'].unique()))
        quarters_user = ['--Select--', 1, 2, 3, 4]
        selected_quarter_user = st.selectbox('Select Quarter for User', quarters_user, index=0)
        selected_state_user = st.selectbox('Select State for User', ['--Select--'] + sorted(aggregated_user_df['States'].unique()))

        # Filter data by selected year, state, and quarter for user
        filtered_user_data = filter_user_data(selected_year_user, selected_state_user, selected_quarter_user)

        # Display select box for brands
        selected_brand = st.selectbox('Select Brand', ['--Select--'] + sorted(aggregated_user_df['Brands'].unique()))

        # Filter data by selected brand
        if selected_brand != '--Select--':
            filtered_user_data = filtered_user_data[filtered_user_data['Brands'] == selected_brand]
        #show_whole_map = st.checkbox('Show whole India map', False)

        # Now, creating the India map using Plotly Express
        india_map_geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Create vertical choropleth map for Transaction Count
        fig_count_user = px.choropleth(
            filtered_user_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Count',
            color_continuous_scale='Reds',
            title='Transaction Count by State in India for Users'
        )
        fig_count_user.update_geos(fitbounds="locations", visible=False)

        # Create vertical choropleth map for Percentage with blue colors
        fig_percentage_user = px.choropleth(
            filtered_user_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Percentage',
            color_continuous_scale='Blues',
            title='Percentage by State in India for Users'
        )
        fig_percentage_user.update_geos(fitbounds="locations", visible=False)

        # Create pie chart for Transaction Count
        fig_pie_count_user = px.pie(
            filtered_user_data,
            values='Transaction_Count',
            names='States',
            title=f'Transaction Count by State in India for Users'
        )
        fig_pie_count_user.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_count_user.update_layout(height=600)

        # Create pie chart for Percentage
        fig_pie_percentage_user = px.pie(
            filtered_user_data,
            values='Percentage',
            names='States',
            title=f'Percentage by State in India for Users'
        )
        fig_pie_percentage_user.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_percentage_user.update_layout(height=600)

        chart_type_user = st.radio('Select Chart Type for User', ['Map for Transaction Count', 'Map for Percentage', 'Pie Chart for Transaction Count', 'Pie Chart for Percentage'])

        if chart_type_user == 'Map for Transaction Count':
            st.plotly_chart(fig_count_user, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_user_data)

        elif chart_type_user == 'Map for Percentage':
            st.plotly_chart(fig_percentage_user, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_user_data)

        elif chart_type_user == 'Pie Chart for Transaction Count':
            st.plotly_chart(fig_pie_count_user)

        else: # 'Pie Chart for Percentage'
            st.plotly_chart(fig_pie_percentage_user)
    
    if choice_topic == "Map Transaction": 
        # Function to filter map transaction data
        def filter_map_transaction_data(selected_year, selected_state, selected_quarter, selected_district):#="--Select--"):
            if selected_year != '--Select--' and selected_state != '--Select--' and selected_quarter != '--Select--' and selected_district != '--Select--':
                return map_transaction_df[(map_transaction_df['Years'] == selected_year) & 
                                        (map_transaction_df['States'] == selected_state) & 
                                        (map_transaction_df['Quarter'] == selected_quarter) &
                                        (map_transaction_df['Districts'] == selected_district)]
            elif selected_year != '--Select--' and selected_state != '--Select--' and selected_quarter != '--Select--':
                return map_transaction_df[(map_transaction_df['Years'] == selected_year) & 
                                        (map_transaction_df['States'] == selected_state) &
                                        (map_transaction_df['Quarter'] == selected_quarter)]
            elif selected_year != '--Select--' and selected_state != '--Select--':
                return map_transaction_df[(map_transaction_df['Years'] == selected_year) & 
                                        (map_transaction_df['States'] == selected_state)]
            elif selected_year != '--Select--' and selected_quarter != '--Select--':
                return map_transaction_df[(map_transaction_df['Years'] == selected_year) & 
                                        (map_transaction_df['Quarter'] == selected_quarter)]
            elif selected_state != '--Select--' and selected_quarter != '--Select--':
                return map_transaction_df[(map_transaction_df['States'] == selected_state) & 
                                        (map_transaction_df['Quarter'] == selected_quarter)]
            elif selected_year != '--Select--':
                return map_transaction_df[map_transaction_df['Years'] == selected_year]
            elif selected_state != '--Select--':
                return map_transaction_df[map_transaction_df['States'] == selected_state]
            elif selected_quarter != '--Select--':
                return map_transaction_df[map_transaction_df['Quarter'] == selected_quarter]
            else:
                return map_transaction_df

        # Dataframe of Map Transaction
        path_3 = "C:/Users/sindh/OneDrive/Desktop/Projects/Phonepe/pulse/data/map/transaction/hover/country/india/state/"
        map_transaction_list = os.listdir(path_3)

        col_3 = {"States": [], "Years": [], "Quarter": [], "Districts": [], "Transaction_Count": [], "Transaction_Amount": []}

        for a in map_transaction_list:  # state
            current_state = path_3 + a + "/"
            aggregated_year_list = os.listdir(current_state)

            for b in aggregated_year_list:  # year
                current_year = current_state + b + "/"
                aggregated_file_list = os.listdir(current_year)

                for c in aggregated_file_list:
                    current_file = current_year + c
                    data = open(current_file, "r")

                    t = json.load(data)
                    for i in t["data"]["hoverDataList"]:
                        name = i["name"]
                        count = i["metric"][0]["count"]
                        amount = i["metric"][0]["amount"]

                        col_3["Districts"].append(name)
                        col_3["Transaction_Count"].append(count)
                        col_3["Transaction_Amount"].append(amount)
                        col_3["States"].append(a)
                        col_3["Years"].append(b)
                        col_3["Quarter"].append(int(c.strip(".json")))
        map_transaction_df = pd.DataFrame(col_3)

        map_transaction_df["States"] = map_transaction_df["States"].str.replace("andaman-&-nicobar-islands", "Andaman-&-Nicobar-Islands")
        map_transaction_df["States"] = map_transaction_df["States"].str.replace("-", " ")
        map_transaction_df["States"] = map_transaction_df["States"].str.title()
        map_transaction_df["States"] = map_transaction_df["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


        # Connect to MySQL database
        db = pymysql.connect(host="localhost", user="root", password="123", port=3306, database="Phonepe")
        cursor = db.cursor()

        # Table for Map Transaction
        cr_query3 = '''CREATE TABLE IF NOT EXISTS map_transaction(
                        States VARCHAR(255),
                        Years INT,
                        Quarters INT,
                        Districts VARCHAR(255),
                        Transaction_Count BIGINT,
                        Transaction_Amount BIGINT)'''
        cursor.execute(cr_query3)
        db.commit()

        # Insert data into the table
        in_query3 = '''INSERT INTO map_transaction(
                        States, Years, Quarters, Districts, Transaction_Count, Transaction_Amount)
                    VALUES (%s, %s, %s, %s, %s, %s)'''
        data = map_transaction_df.values.tolist()
        cursor.executemany(in_query3, data)
        db.commit()


        # Display select boxes for years, states, and quarters for map transaction data
        selected_year_map_transaction = st.selectbox('Select Year for Map Transaction', ['--Select--'] + sorted(map_transaction_df['Years'].unique()))
        selected_state_map_transaction = st.selectbox('Select State for Map Transaction', ['--Select--'] + sorted(map_transaction_df['States'].unique()))
        quarters_map_transaction = ['--Select--', 1, 2, 3, 4]
        selected_quarter_map_transaction = st.selectbox('Select Quarter for Map Transaction', quarters_map_transaction, index=0)

        # Filter data by selected year, state, and quarter for map transaction data
        filtered_map_transaction_data = filter_map_transaction_data(selected_year_map_transaction, selected_state_map_transaction, selected_quarter_map_transaction, '--Select--')

        # Display select box for districts
        selected_district = st.selectbox('Select District', ['--Select--'] + sorted(filtered_map_transaction_data['Districts'].unique()))

        # Filter map transaction data by selected district
        if selected_district != '--Select--':
            filtered_district_data = filter_map_transaction_data(selected_year_map_transaction, selected_state_map_transaction, selected_quarter_map_transaction, selected_district)
        else:
            filtered_district_data = filtered_map_transaction_data

        # Filter tabular view based on selected district
        if selected_district != '--Select--':
            # Filter the DataFrame based on the selected district
            filtered_tabular_data = filtered_map_transaction_data[filtered_map_transaction_data['Districts'] == selected_district]
        else:
            # If no district is selected, display the entire filtered map transaction data
            filtered_tabular_data = filtered_map_transaction_data

        # Display the tabular view
        #st.write(filtered_tabular_data)


        #creating the India map using Plotly Express
        india_map_geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Create vertical choropleth map for Transaction Count
        fig_count_transaction = px.choropleth(
            filtered_map_transaction_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Count',
            color_continuous_scale='Reds',
            title='Transaction Count by State in India'
        )
        fig_count_transaction.update_geos(fitbounds="locations", visible=False)

        # Create vertical choropleth map for Transaction Amount
        fig_amount_transaction = px.choropleth(
            filtered_map_transaction_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Amount',
            color_continuous_scale='Blues',
            title='Transaction Amount by State in India'
        )
        fig_amount_transaction.update_geos(fitbounds="locations", visible=False)

        # Create pie chart for Transaction Count
        fig_pie_count_transaction = px.pie(
            filtered_map_transaction_data,
            values='Transaction_Count',
            names='Districts',
            title=f'Transaction Count by State in India'
        )
        fig_pie_count_transaction.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_count_transaction.update_layout(height=600)

        # Create pie chart for Transaction Amount
        fig_pie_amount_transaction = px.pie(
            filtered_map_transaction_data,
            values='Transaction_Amount',
            names='Districts',
            title=f'Transaction Amount by State in India'
        )
        fig_pie_amount_transaction.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_amount_transaction.update_layout(height=600)

        chart_type_transaction = st.radio('Select Chart Type for Transaction', ['Map for Transaction Count','Pie Chart for Transaction Count',  'Map for Transaction Amount', 'Pie Chart for Transaction Amount'])

        if chart_type_transaction == 'Map for Transaction Count':
            st.plotly_chart(fig_count_transaction, use_container_width=True)
            # Display the tabular view
            st.write(filtered_tabular_data) 

        elif chart_type_transaction == 'Pie Chart for Transaction Count':
            st.plotly_chart(fig_pie_count_transaction)
            

        elif chart_type_transaction == 'Map for Transaction Amount':
            st.plotly_chart(fig_amount_transaction, use_container_width=True)
            st.write(filtered_tabular_data)     

        else: # 'Pie Chart for Transaction Amount'
            st.plotly_chart(fig_pie_amount_transaction)


    if choice_topic == "Map User":
        # Function to filter map user data
        def filter_map_user_data(selected_state, selected_year, selected_quarter, selected_district):
            if selected_year != '--Select--' and selected_state != '--Select--' and selected_quarter != '--Select--' and selected_district != '--Select--':
                return map_user_df[(map_user_df['Years'] == int(selected_year)) & 
                                        (map_user_df['States'] == selected_state) & 
                                        (map_user_df['Quarter'] == int(selected_quarter)) &
                                        (map_user_df['Districts'] == selected_district)]
            elif selected_year != '--Select--' and selected_state != '--Select--' and selected_quarter != '--Select--':
                return map_user_df[(map_user_df['Years'] == int(selected_year)) & 
                                        (map_user_df['States'] == selected_state) &
                                        (map_user_df['Quarter'] == int(selected_quarter))]
            elif selected_year != '--Select--' and selected_state != '--Select--':
                return map_user_df[(map_user_df['Years'] == int(selected_year)) & 
                                        (map_user_df['States'] == selected_state)]
            elif selected_year != '--Select--' and selected_quarter != '--Select--':
                return map_user_df[(map_user_df['Years'] == int(selected_year)) & 
                                        (map_user_df['Quarter'] == int(selected_quarter))]
            elif selected_state != '--Select--' and selected_quarter != '--Select--':
                return map_user_df[(map_user_df['States'] == selected_state) & 
                                        (map_user_df['Quarter'] == int(selected_quarter))]
            elif selected_year != '--Select--':
                return map_user_df[map_user_df['Years'] == int(selected_year)]
            elif selected_state != '--Select--':
                return map_user_df[map_user_df['States'] == selected_state]
            elif selected_quarter != '--Select--':
                return map_user_df[map_user_df['Quarter'] == int(selected_quarter)]
            else:
                return map_user_df


        # Dataframe of Map User
        path_4 = "C:/Users/sindh/OneDrive/Desktop/Projects/Phonepe/pulse/data/map/user/hover/country/india/state/"
        map_user_list = os.listdir(path_4)

        col_4 = {"States": [], "Years": [], "Quarter": [], "Districts": [], "Registered_Users": [], "App_Opens": []}

        for a in map_user_list:#state
            current_state = path_4 + a + "/"
            aggregated_year_list = os.listdir(current_state)

            for b in aggregated_year_list:#year
                current_year = current_state + b + "/"
                aggregated_file_list = os.listdir(current_year)

                for c in aggregated_file_list:
                    current_file = current_year + c
                    data = open(current_file,"r")

                    w = json.load(data)
                    for i in w["data"]["hoverData"].items():
                        district = i[0]
                        reg_user = i[1]["registeredUsers"]
                        app_open = i[1]["appOpens"]
                        col_4["Districts"].append(district)
                        col_4["Registered_Users"].append(reg_user)
                        col_4["App_Opens"].append(app_open)
                        col_4["States"].append(a)
                        col_4["Years"].append(int(b))
                        col_4["Quarter"].append(int(c.strip(".json")))

        map_user_df = pd.DataFrame(col_4)

        map_user_df["States"] = map_user_df["States"].str.replace("andaman-&-nicobar-islands", "Andaman-&-Nicobar-Islands")
        map_user_df["States"] = map_user_df["States"].str.replace("-"," ")
        map_user_df["States"] = map_user_df["States"].str.title()
        map_user_df["States"] = map_user_df["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Connect to MySQL database
        db = pymysql.connect(host="localhost", user="root", password="123", port=3306, database="Phonepe")
        cursor = db.cursor()

        # Table for Map User
        cr_query4 = '''CREATE TABLE IF NOT EXISTS map_user(
                        States VARCHAR(255),
                        Years INT,
                        Quarters INT,
                        Districts VARCHAR(255),
                        Registered_Users BIGINT,
                        App_Opens BIGINT)'''
        cursor.execute(cr_query4)
        db.commit()

        # Insert data into the table
        in_query4 = '''INSERT INTO map_user(
                        States, Years, Quarters, Districts, Registered_Users, App_Opens)
                    VALUES (%s, %s, %s, %s, %s, %s)'''
        data = map_user_df.values.tolist()
        cursor.executemany(in_query4, data)
        db.commit()

        # Display select boxes for years, states, and quarters for map transaction data
        selected_year_map_user = st.selectbox('Select Year for Map User', ['--Select--'] + sorted(map_user_df['Years'].unique()))
        selected_state_map_user = st.selectbox('Select State for Map User', ['--Select--'] + sorted(map_user_df['States'].unique()))
        quarters_map_user = ['--Select--', 1, 2, 3, 4]
        selected_quarter_map_user = st.selectbox('Select Quarter for Map User', quarters_map_user, index=0)

        # Filter map transaction data based on selected filters
        filtered_map_user_data = filter_map_user_data(selected_state_map_user, selected_year_map_user, selected_quarter_map_user, '--Select--')

        # Display select box for districts
        selected_district = st.selectbox('Select District', ['--Select--'] + sorted(filtered_map_user_data['Districts'].unique()))

        # Filter map transaction data by selected district
        if selected_district != '--Select--':
            filtered_district_data = filter_map_user_data(selected_state_map_user, selected_year_map_user, selected_quarter_map_user, selected_district)
        else:
            filtered_district_data = filtered_map_user_data

        # Filter tabular view based on selected district
        if selected_district != '--Select--':
            # Filter the DataFrame based on the selected district
            filtered_tabular_data = filtered_map_user_data[filtered_map_user_data['Districts'] == selected_district]
        else:
            # If no district is selected, display the entire filtered map transaction data
            filtered_tabular_data = filtered_map_user_data

        # Aggregate registered users by state
        registered_users_by_state = map_user_df.groupby('States')['Registered_Users'].sum().reset_index()

        india_map_geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Create vertical choropleth map for Registered Users
        fig_registered_users_state = px.choropleth(
            filtered_map_user_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Registered_Users',
            color_continuous_scale='Reds',
            title='Registered Users by State in India for Top Users'
        )
        fig_registered_users_state.update_geos(fitbounds="locations", visible=False)

        # Create choropleth map for app opens by state
        fig_app_opens_state = px.choropleth(
            filtered_map_user_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='App_Opens',
            color_continuous_scale='Blues',
            hover_name='States',
            title='App Opens by State in India'
        )

        # Update map layout
        fig_app_opens_state.update_geos(fitbounds="locations", visible=False)

        # Create pie chart for registered users
        fig_pie_reg_users = px.pie(
            filtered_district_data,
            values='Registered_Users',
            names='Districts',
            title='Registered Users Distribution by District'
        )

        # Create pie chart for app opens
        fig_pie_app_opens = px.pie(
            filtered_district_data,
            values='App_Opens',
            names='Districts',
            title='App Opens Distribution by District'
        )
        # Aggregate app opens by state
        app_opens_by_state = map_user_df.groupby('States')['App_Opens'].sum().reset_index()

        # Display the visualizations based on selected options
        chart_type_map_user = st.radio('Select Chart Type for Map User', ['Map for Registered Users', 'Map for App Opens', 'Pie Chart for Registered Users', 'Pie Chart for App Opens'])

        if chart_type_map_user == 'Map for Registered Users':
            st.plotly_chart(fig_registered_users_state)
            # Display tabular view along with map
            st.write(filtered_district_data)

        elif chart_type_map_user == 'Map for App Opens':
            st.plotly_chart(fig_app_opens_state)
            # Display tabular view along with map
            st.write(filtered_district_data)
            
        elif chart_type_map_user == 'Pie Chart for Registered Users':
            st.plotly_chart(fig_pie_reg_users)

        elif chart_type_map_user == 'Pie Chart for App Opens':
            st.plotly_chart(fig_pie_app_opens)
            
    if choice_topic == "Top Transaction":
        # Function to filter top transaction data
        def filter_top_transaction_data(selected_year, selected_state, selected_quarter, selected_pincode):
            filtered_data = top_transaction_df.copy()
            
            # Apply filters
            if selected_year != '--Select--':
                filtered_data = filtered_data[filtered_data['Years'] == selected_year]
            if selected_state != '--Select--':
                filtered_data = filtered_data[filtered_data['States'] == selected_state]
            if selected_quarter != '--Select--':
                filtered_data = filtered_data[filtered_data['Quarter'] == selected_quarter]
            if selected_pincode != '--Select--':
                filtered_data = filtered_data[filtered_data['Pincodes'] == selected_pincode]
            
            return filtered_data

        # Dataframe of Top Transaction
        path_5 = "C:/Users/sindh/OneDrive/Desktop/Projects/Phonepe/pulse/data/top/transaction/country/india/state/"
        top_transaction_list = os.listdir(path_5)

        col_5 = {"States": [], "Years": [], "Quarter": [], "Pincodes": [], "Transaction_Count": [], "Transaction_Amount": []}

        for a in top_transaction_list:  # state
            current_state = path_5 + a + "/"
            aggregated_year_list = os.listdir(current_state)

            for b in aggregated_year_list:  # year
                current_year = current_state + b + "/"
                aggregated_file_list = os.listdir(current_year)

                for c in aggregated_file_list:
                    current_file = current_year + c
                    data = open(current_file, "r")

                    q = json.load(data)
                    for i in q["data"]["pincodes"]:
                        entity_name = i["entityName"]
                        count = i["metric"]["count"]
                        amount = i["metric"]["amount"]
                        col_5["Pincodes"].append(entity_name)
                        col_5["Transaction_Count"].append(count)
                        col_5["Transaction_Amount"].append(amount)
                        col_5["States"].append(a)
                        col_5["Years"].append(b)
                        col_5["Quarter"].append(int(c.strip(".json")))

        top_transaction_df = pd.DataFrame(col_5)

        top_transaction_df["States"] = top_transaction_df["States"].str.replace("andaman-&-nicobar-islands",
                                                                                "Andaman-&-Nicobar-Islands")
        top_transaction_df["States"] = top_transaction_df["States"].str.replace("-", " ")
        top_transaction_df["States"] = top_transaction_df["States"].str.title()
        top_transaction_df["States"] = top_transaction_df["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu",
                                                                                "Dadra and Nagar Haveli and Daman and Diu")

        # Connect to MySQL database
        db = pymysql.connect(host="localhost", user="root", password="123", port=3306, database="Phonepe")
        cursor = db.cursor()

        # Table for Top Transaction
        cr_query5 = '''CREATE TABLE IF NOT EXISTS top_transaction(
                        States VARCHAR(255),
                        Years INT,
                        Quarters INT,
                        Pincodes INT,
                        Transaction_Count VARCHAR(255),
                        Transaction_Amount BIGINT)'''
        cursor.execute(cr_query5)
        db.commit()

        # Insert data into the table
        in_query5 = '''INSERT INTO top_transaction(
                        States, Years, Quarters, Pincodes, Transaction_Count, Transaction_Amount)
                    VALUES (%s, %s, %s, %s, %s, %s)'''
        data = top_transaction_df.values.tolist()
        cursor.executemany(in_query5, data)
        db.commit()


        # Display select boxes for filtering map user data
        selected_year_top_transaction = st.selectbox('Select Year for Top Users', ['--Select--'] + sorted(top_transaction_df['Years'].unique()))
        quarters_top_transaction = ['--Select--', 1, 2, 3, 4]
        selected_quarter_top_transaction = st.selectbox('Select Quarter for Top Users', quarters_top_transaction, index=0)
        selected_state_top_transaction = st.selectbox('Select State for Top Users', ['--Select--'] + sorted(top_transaction_df['States'].unique()))

        # Filter out None values from Pincodes column and convert to string
        if selected_state_top_transaction != '--Select--':
            valid_pincodes = top_transaction_df[top_transaction_df['States'] == selected_state_top_transaction]['Pincodes'].dropna().astype(str)
        else:
            valid_pincodes = top_transaction_df['Pincodes'].dropna().astype(str)

        # Sort the pincode values and add '--Select--' option
        pincode_options = ['--Select--'] + sorted(valid_pincodes.unique())

        selected_pincode_top_transaction = st.selectbox('Select Pincode for Top Users', pincode_options)

        # Filter top user data based on selected filters
        filtered_top_transaction_data = filter_top_transaction_data(selected_year_top_transaction, selected_state_top_transaction, selected_quarter_top_transaction, selected_pincode_top_transaction)

        # Now, creating the India map using Plotly Express
        india_map_geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Create vertical choropleth map for Transaction Count
        fig_count_top_transaction = px.choropleth(
            filtered_top_transaction_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Count',
            color_continuous_scale='Reds',
            title='Transaction Count by State in India for Top Transactions'
        )
        fig_count_top_transaction.update_geos(fitbounds="locations", visible=False)

        # Create vertical choropleth map for Transaction Amount with blue colors
        fig_amount_top_transaction = px.choropleth(
            filtered_top_transaction_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Transaction_Amount',
            color_continuous_scale='Blues',
            title='Transaction Amount by State in India for Top Transactions'
        )
        fig_amount_top_transaction.update_geos(fitbounds="locations", visible=False)

        # Create pie chart for Transaction Count
        fig_pie_count_top_transaction = px.pie(
            filtered_top_transaction_data,
            values='Transaction_Count',
            names='States',
            title=f'Transaction Count by State in India for Top Transactions'
        )
        fig_pie_count_top_transaction.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_count_top_transaction.update_layout(height=600)

        # Create pie chart for Transaction Amount
        fig_pie_amount_top_transaction = px.pie(
            filtered_top_transaction_data,
            values='Transaction_Amount',
            names='States',
            title=f'Transaction Amount by State in India for Top Transactions'
        )
        fig_pie_amount_top_transaction.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_amount_top_transaction.update_layout(height=600)

        # Display the visualizations based on selected options
        chart_type_top_transaction = st.radio('Select Chart Type for Top Transactions', ['Map for Transaction Count', 'Map for Transaction Amount', 'Pie Chart for Transaction Count', 'Pie Chart for Transaction Amount'])

        if chart_type_top_transaction == 'Map for Transaction Count':
            st.plotly_chart(fig_count_top_transaction, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_top_transaction_data)

        elif chart_type_top_transaction == 'Map for Transaction Amount':
            st.plotly_chart(fig_amount_top_transaction, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_top_transaction_data)

        elif chart_type_top_transaction == 'Pie Chart for Transaction Count':
            st.plotly_chart(fig_pie_count_top_transaction)

        else:  # 'Pie Chart for Transaction Amount'
            st.plotly_chart(fig_pie_amount_top_transaction)

    if choice_topic == "Top User":
        # Function to filter top user data
        def filter_top_user_data(selected_year, selected_state, selected_quarter, selected_pincode):
            filtered_data = top_user_df.copy()
            
            # Apply filters
            if selected_year != '--Select--':
                filtered_data = filtered_data[filtered_data['Years'] == selected_year]
            if selected_state != '--Select--':
                filtered_data = filtered_data[filtered_data['States'] == selected_state]
            if selected_quarter != '--Select--':
                filtered_data = filtered_data[filtered_data['Quarter'] == selected_quarter]
            if selected_pincode != '--Select--':
                filtered_data = filtered_data[filtered_data['Pincodes'] == selected_pincode]
            
            return filtered_data

        # Dataframe of Top Users
        path_6 ="C:/Users/sindh/OneDrive/Desktop/Projects/Phonepe/pulse/data/top/user/country/india/state/"
        top_user_list = os.listdir(path_6)

        col_6 = {"States": [], "Years": [], "Quarter": [], "Pincodes": [], "Registered_Users": []}

        for a in top_user_list:#state
            current_state = path_6 + a + "/"
            aggregated_year_list = os.listdir(current_state)

            for b in aggregated_year_list:#year
                current_year = current_state + b + "/"
                aggregated_file_list = os.listdir(current_year)

                for c in aggregated_file_list:
                    current_file = current_year + c
                    data = open(current_file,"r")

                    e = json.load(data)
                    for i in e["data"]["pincodes"]:
                        entity_name = i["name"]
                        reg_user = i["registeredUsers"]
                        col_6["Pincodes"].append(entity_name)
                        col_6["Registered_Users"].append(reg_user)
                        col_6["States"].append(a)
                        col_6["Years"].append(b)
                        col_6["Quarter"].append(int(c.strip(".json")))

        top_user_df = pd.DataFrame(col_6)

        top_user_df["States"] = top_user_df["States"].str.replace("andaman-&-nicobar-islands", "Andaman-&-Nicobar-Islands")
        top_user_df["States"] = top_user_df["States"].str.replace("-"," ")
        top_user_df["States"] = top_user_df["States"].str.title()
        top_user_df["States"] = top_user_df["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Connect to MySQL database
        db = pymysql.connect(host="localhost", user="root", password="123", port=3306, database="Phonepe")
        cursor = db.cursor()

        # Table for Top Users
        cr_query6 = '''CREATE TABLE IF NOT EXISTS top_user(
                        States VARCHAR(255),
                        Years INT,
                        Quarters INT,
                        Pincodes INT,
                        Registered_Users BIGINT)'''
        cursor.execute(cr_query6)
        db.commit()

        # Insert data into the table
        in_query6 = '''INSERT INTO top_user(
                        States, Years, Quarters, Pincodes, Registered_Users)
                    VALUES (%s, %s, %s, %s, %s)'''
        data = top_user_df.values.tolist()
        cursor.executemany(in_query6, data)
        db.commit()

        # Display select boxes for filtering map user data
        selected_year_top_user = st.selectbox('Select Year for Top Users', ['--Select--'] + sorted(top_user_df['Years'].unique()))
        quarters_top_user = ['--Select--', 1, 2, 3, 4]
        selected_quarter_top_user = st.selectbox('Select Quarter for Top Users', quarters_top_user, index=0)
        selected_state_top_user = st.selectbox('Select State for Top Users', ['--Select--'] + sorted(top_user_df['States'].unique()))

        # Filter out None values from Pincodes column and convert to string
        if selected_state_top_user != '--Select--':
            valid_pincodes = top_user_df[top_user_df['States'] == selected_state_top_user]['Pincodes'].dropna().astype(str)
        else:
            valid_pincodes = top_user_df['Pincodes'].dropna().astype(str)

        # Sort the pincode values and add '--Select--' option
        pincode_options = ['--Select--'] + sorted(valid_pincodes.unique())

        selected_pincode_top_user = st.selectbox('Select Pincode for Top Users', pincode_options)

        # Filter top user data based on selected filters
        filtered_top_user_data = filter_top_user_data(selected_year_top_user, selected_state_top_user, selected_quarter_top_user, selected_pincode_top_user)

        # Now, creating the India map using Plotly Express
        india_map_geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Create vertical choropleth map for Registered Users
        fig_registered_users_top_user = px.choropleth(
            filtered_top_user_data,
            geojson=india_map_geojson,
            featureidkey='properties.ST_NM',
            locations='States',
            color='Registered_Users',
            color_continuous_scale='Reds',
            title='Registered Users by State in India for Top Users'
        )
        fig_registered_users_top_user.update_geos(fitbounds="locations", visible=False)

        # Create pie chart for Registered Users
        fig_pie_registered_users_top_user = px.pie(
            filtered_top_user_data,
            values='Registered_Users',
            names='States',
            title=f'Registered Users by State in India for Top Users'
        )
        fig_pie_registered_users_top_user.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_registered_users_top_user.update_layout(height=600)

        # Display the visualizations based on selected options
        chart_type_top_user = st.radio('Select Chart Type for Top Users', ['Map for Registered Users', 'Pie Chart for Registered Users'])

        if chart_type_top_user == 'Map for Registered Users':
            st.plotly_chart(fig_registered_users_top_user, use_container_width=True)
            # Display tabular view along with map
            st.write(filtered_top_user_data)

        else:  # 'Pie Chart for Registered Users'
            st.plotly_chart(fig_pie_registered_users_top_user)


