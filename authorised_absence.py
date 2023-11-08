from pyspark.sql.functions import col

#n authorised absences because of medical appointments or illness in the time period 2017-2018
def auth_leave_schooltype(schoolType, df):
    filtered_df = df[df['school_type'].isin(schoolType)]
    filtered_df = filtered_df.filter((filtered_df['geographic_level'] == 'National') & (filtered_df['time_period'] == '201718'))
    filtered_df = filtered_df.withColumn('Total_auth_absentees',col('sess_auth_appointments') + col('sess_auth_illness'))
    filtered_df = filtered_df.select(['time_period','school_type','Total_auth_absentees'])
    return filtered_df