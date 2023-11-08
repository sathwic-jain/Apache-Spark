

#    author: 220026989
#    Console based application for CS5202 coursework


from create import create
from enrolls_by_la import display_enrol_by_la
from authorised_absence import auth_leave_schooltype
from unauth_absence import unauth_absentees
from top_three_auth_absence import top_three_auth_absence
from compare_two_la import compare_local_auth
from region_performance_2006_18 import regional_performance_2006_2018
import matplotlib.pyplot as plt

# get the spark context and pyspark dataframe
spark, df_pyspark = create()

# options to perform various operations
while True:
    user_choice = input('''
    Enter your choice :
         1. search the dataset by the local authority
         2. search the dataset by school type
         3. search for all unauthorised absences in a certain year
         4. List the top 3 reasons for authorised absences in each year
         5. Compare two local authorities for any session in a given year
         6. Chart - explore the performance of regions in England from 2006-2018
         7. Quit

Input :  ''')

    match user_choice:
        case "1":
            # PART A - 1
            # Get user input for the local authorities to search for
            la_input = input("Enter local authorities: ")
            la_list = la_input.split(",")
            la_list = [item.strip() for item in la_list]  # strip the empty spaces if any
            # Display the number of pupil enrolments for the specified local authorities
            result = display_enrol_by_la(la_list, df_pyspark)
            result.show()
        case "2":
            # PART A - 2
            # Get user input for the school type to search for
            school_type = input("Enter the school type: ")
            # Display total number of pupils who were given authorised absences because of
            # medical appointments or illness in the time period 2017-2018.
            result = auth_leave_schooltype(school_type, df_pyspark)
            result.show(truncate=False)
        case "3":
            # PART A - 3
            # Get user input for the time_period and whether "region" or "local"
            time_period = input("Enter the time-period: ")
            search_item = input("Enter the level to be searched for (Regional or Local authority): ").strip()
            # Display all unauthorised absences in a certain year
            result = unauth_absentees(time_period, search_item, df_pyspark)
            result.show()
        case "4":
            # PART A - 4
            result = top_three_auth_absence(df_pyspark, spark)
            result.show(truncate=False)
        case "5":
            while True:
                # PART B - 1
                option = input('''
                choose an option to compare(1, 2, 3, 4, 5, 6)
                     1. Authorized Reasons Sessions
                     2. Overall Absence Rate
                     3. Authorized Absence Rate
                     4. Unauthorized Absence Rate
                     5. Exit this menu

                Input :  ''')
                option = int(option)
                if(option == 5):
                    break
                # Get user input for the local authorities to compare
                la_input = input("Enter local authorities (comma-separated): ")
                la_list = la_input.split(",")
                la_list = [item.strip() for item in la_list]  # strip the empty spaces if any
                # Get user input for the year to compare
                year = input("Enter the year to compare (Format eg:- 200708): ")
                # Display the number of pupil enrolments for the specified local authorities
                fig = compare_local_auth(year,la_list, option,df_pyspark)
                plt.figure(fig)
                plt.show()
                
        case "6":
            # PART b - 2
            fig1, fig2 = regional_performance_2006_2018(df_pyspark)
            # Show the first figure
            plt.figure(fig1)
            # Show the second figure
            plt.figure(fig2)
            plt.show()
        case "7":
            print("Quitting")
            break