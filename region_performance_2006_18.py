from pyspark.sql.functions import greatest, col, asc
import pandas as pd
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

def regional_performance_2006_2018(df):
    filtered_df = df.filter((df['geographic_level'] == 'Regional') & (df['country_name'] == 'England') & (df['school_type'] == 'Total'))
    #all_time_period = filtered_df.select('time_period').distinct()
    #to check the whole time periods
    #made sure that all the time periods are to be used and no period is missing
    # subtract total absence from total possible session to get the sessions having full attendance
    filtered_df = filtered_df.orderBy("region_name", asc("time_period")).withColumn("sess_attendance", (col("sess_possible") - col("sess_overall")))
    #divide the sess_attendance by sess_possible to get the attendance rate
    filtered_df = filtered_df.withColumn('attendance_rate',col('sess_attendance')/col('sess_possible'))
    filtered_df = filtered_df.select(['time_period','region_name','sess_attendance','attendance_rate'])
    # get the distinct values from the region
    regions = filtered_df.select('region_name').distinct().collect()
    colors = ['blue', 'orange', 'green', 'red', 'purple', 'brown', 'pink', 'gray', 'olive', 'yellow']
    # iterate over the list of Row objects and extract the values
    data = [row['region_name'] for row in regions]
    #plot the attendance and attendance rate for each region
    fig1 = plot_for_attendance_rate(data, filtered_df)
    fig2 = plot_for_attendance(data, filtered_df)
    return fig1, fig2

def plot_for_attendance(data, filtered_df):   
    #Create a figure and axis object
    fig1 = plt.figure(figsize=(12, 6))
    ax1 = fig1.add_subplot(111)

    #Create a list of colors for each region
    colors = ['red', 'blue', 'green', 'purple', 'orange','brown','pink','gray','olive','yellow']

    #Loop over each region
    for i, region in enumerate(data):
        # Filter the dataframe for the current region
        region_data = filtered_df.filter(F.col('region_name') == region)
        # Convert PySpark dataframe to Pandas dataframe
        region_pd = region_data.select('time_period', 'sess_attendance').toPandas()
        # Plot the region data
        ax1.plot(region_pd['time_period'], region_pd['sess_attendance'], label=region, color=colors[i % len(colors)])

    #Set the x and y axis labels and title
    ax1.set_xlabel('Year', labelpad=10)
    ax1.set_ylabel('Attendance Noted', labelpad=10)
    ax1.set_title('Attendance Noted by Region and Year')

    #Add a legend and adjust its position
    ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.subplots_adjust(right=0.7)

    return fig1

def plot_for_attendance_rate(data, filtered_df):
    #2nd plot
    fig2 = plt.figure(figsize=(12, 6))
    ax2 = fig2.add_subplot(111)

    #Create a list of colors for each region
    colors = ['red', 'blue', 'green', 'purple', 'orange','brown','pink','gray','olive','yellow']

    #Loop over each region
    for i, region in enumerate(data):
        # Filter the dataframe for the current region
        region_data = filtered_df.filter(F.col('region_name') == region)
        # Convert PySpark dataframe to Pandas dataframe
        region_pd = region_data.select('time_period', 'attendance_rate').toPandas()
        # Plot the region data
        ax2.plot(region_pd['time_period'], region_pd['attendance_rate'], label=region, color=colors[i % len(colors)])

    #Set the x and y axis labels and title
    ax2.set_xlabel('Year', labelpad=10)
    ax2.set_ylabel('Attendance Rate', labelpad=10)
    ax2.set_title('Attendance Rate by Region and Year')
    #Add a legend and adjust its position
    ax2.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.subplots_adjust(right=0.7)

    return fig2