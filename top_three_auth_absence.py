#List top three reasons for authorised absences each year
from pyspark.sql.functions import greatest, col, when, desc


def top_three_auth_absence(df, spark):
    #filtered_df = df.select([col for col in df.columns if col.startswith('sess_auth_'), 'time_period'])
    filtered_df = df.filter((df['geographic_level'] == 'National') & (df['school_type'] == 'Total'))
    filtered_df = filtered_df.select(['time_period'] + [col for col in df.columns if col.startswith('sess_auth_')])
    filtered_df = filtered_df.drop('sess_auth_totalreasons')
    
    # fill any values having null with 0
    filtered_df = filtered_df.fillna(0)

    top_3_list = []
    for row in filtered_df.collect():
        temp_dict = {}
        row_dict = row.asDict()
        time_period = row_dict["time_period"]
        temp_dict['Time Period'] = time_period
        row_dict.pop("time_period")
         # after sorting the top 3 reasons will be in the index 0, 1, 2 repectively
        sorted_row = sorted(row_dict, key=row_dict.get, reverse=True)
        temp_dict['Top Reason'] = sorted_row[0]
        temp_dict['Second Reason'] = sorted_row[1]
        temp_dict['Third Reason'] = sorted_row[2]
        top_3_list.append(temp_dict)

    
    # create a new PrettyTable object with the column names as Reaon1, Reason2, Reason3
    #table = PrettyTable(top_3_list[0].keys())

    # creating a dataframe
    table_df = spark.createDataFrame(top_3_list)

    return table_df