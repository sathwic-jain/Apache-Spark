#Given a list of local authorities, display in a well-formatted fashion
#the number of pupil enrolments in each local authority by time period

def display_enrol_by_la(la_list, df):
    #filtered_df = df.filter(df['geographic_level'] == 'Local authority')
    filtered_df = df.filter((df['geographic_level'] == 'Local authority') & (df['school_type'] == 'Total'))
    filtered_df = filtered_df[filtered_df['la_name'].isin(la_list)]
    final_filtered = filtered_df.select(['time_period','la_name','enrolments'])
    #final_filtered.show()
    return final_filtered