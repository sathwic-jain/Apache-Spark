#  author: 220026989
#
#  This code is for the advanced part of the coursework
#  The file is used for the creation of a web-app using streamlit library
from create import create_for_streamlit
from enrolls_by_la import display_enrol_by_la
from authorised_absence import auth_leave_schooltype
from unauth_absence import unauth_absentees
from top_three_auth_absence import top_three_auth_absence
from compare_two_la import compare_local_auth
from region_performance_2006_18 import regional_performance_2006_2018
import streamlit as st



#Initially getting the input for the file path
path = st.text_input("Enter the path to the csv file: ")
st.text("If you are using the default path, leave the input empty, just enter empty")

# get the spark context and pyspark dataframe
spark, df_pyspark = create_for_streamlit(path)

# title of the web application
st.title("Absent data of pupils in England")

# display CSV
st.subheader("Display CSV")
st.dataframe(df_pyspark)

# let the user select from the given options
st.subheader("Choose an option")
options = ["Options",
           "search the dataset by the local authority",
           "search the dataset by school type",
           "search for all unauthorised absences in a certain year",
           "List the top 3 reasons for authorised absences in each year",
           "compare two local authorities in a given year",
           "Chart/explore the performance of regions in England from 2006-2018",
           "QUIT"]
choice = st.selectbox("Select an option", options)  # save the user choice to a var

# choose a functionality according to the user selection
if choice == "search the dataset by the local authority":
    # PART 1, (1.1)
    # Get user input for the local authorities to search for
    la_input = st.text_input("Enter local authorities (comma-separated): ")
    if la_input:
        la_list = la_input.split(",")
        la_list = [item.strip() for item in la_list]  # strip the empty spaces if any
        # Display the number of pupil enrolments for the specified local authorities
        result = display_enrol_by_la(la_list, df_pyspark)
        st.dataframe(result)
elif choice == "search the dataset by school type":
    # PART 1, (1.2)
    # Get user input for the school type to be searched for
    school_type = st.text_input("Enter the school type: ")
    if school_type:
        # Display total number of pupils who were given authorised absences because of
        # medical appointments or illness in the time period 2017-2018.
        result = auth_leave_schooltype(school_type, df_pyspark)
        st.dataframe(result)
elif choice == "search for all unauthorised absences in a certain year":
    # PART 1, (1.3)
    # Get user input for the time_period and whether "region" or "local"
    time_period = st.text_input("Enter the time-period: ")
    search_item = st.text_input("Enter the level to be searched for (Regional or Local authority): ").strip()
    if time_period and search_item:
        # Display all unauthorised absences in a certain year
        result = unauth_absentees(time_period, search_item, df_pyspark)
        st.dataframe(result)
elif choice == "List the top 3 reasons for authorised absences in each year":
    # PART 1, (1.4)
    # Display the top 3 reasons for authorised absences in each year
    result_df = top_three_auth_absence(df_pyspark, spark)
    st.dataframe(result_df)
elif choice == "compare two local authorities in a given year":
    # PART 2, (2.1)
    user_option = ["Options",
           "Authorized Reasons for Absence",
           "Overall Absence Rate",
           "Authorised Absence Rate",
           "Unauthorized Absence Rate"]
    option = st.selectbox("Select an option", user_option)  # save the user choice to a var
    year = st.text_input("Enter the year to compare: ")
    la1 = st.text_input("Enter the first local authority to compare: ")
    la2 = st.text_input("Enter the second local authority to compare: ")
    if option == "Authorized Reasons for Absence":
        option_select = 1
    elif option == "Overall Absence Rate":
        option_select = 2
    elif option == "Authorised Absence Rate":
        option_select = 3
    elif option == "Unauthorized Absence Rate":
        option_select = 4
    if year and la1 and la2:
        la_list = [la1, la2]
        plt = compare_local_auth(year, la_list, option_select, df_pyspark)
        st.pyplot(plt)
elif choice == "Chart/explore the performance of regions in England from 2006-2018":
    # PART 2, (2.2)
    fig1, fig2 = regional_performance_2006_2018(df_pyspark)
    # Display the first plot
    st.pyplot(fig1)
    # Display the second plot
    st.pyplot(fig2)
elif choice == "QUIT":
    st.stop()