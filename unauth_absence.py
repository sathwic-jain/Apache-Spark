def unauth_absentees(search_year, search_region, df):
    filtered_df = df.filter((df['geographic_level'] == search_region) & (df['time_period'] == search_year) \
                            & (df['school_type'] == 'Total'))
    if (search_region == 'Regional'):
        filtered_df = filtered_df.select(['region_name','sess_unauthorised'])
    elif (search_region == 'Local authority'):
        filtered_df = filtered_df.select(['la_name','sess_unauthorised'])
    return filtered_df