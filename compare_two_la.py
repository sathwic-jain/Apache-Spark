# Create visualizations to present the data
import matplotlib.pyplot as plt
from pyspark.sql.functions import greatest, col

def compare_local_auth(year,la_list_compare,toCompare,df):
    la1 = la_list_compare[0]
    la2 = la_list_compare[1]
    
    match toCompare:
        case 1:
            #number of authorized reasons sessions
            comparing = 'sess_authorised'
            text = 'Number of Authorized Reasons Sessions by Local Authority -' + str(year)
        case 2:
            #overall absence rate
            comparing = 'sess_overall_percent'
            text = 'Overall Absence Rate by Local Authority -' + str(year)
        case 3:
            #authorised absence rate
            comparing = 'sess_authorised_percent'
            text = 'Authorized Absence Rate by Local Authority -' + str(year)
        case 4:
            #unauthorised absence rate
            comparing = 'sess_unauthorised_percent'
            text = 'Unauthorized Absence Rate by Local Authority -' + str(year)
            
    filtered_df = df.filter((df['geographic_level'] == 'Local authority') & (df['school_type'] == 'Total') & (df['time_period'] == year))
    filtered_df = filtered_df.filter((df['la_name'] == la1) | (df['la_name'] == la2))
    comparing_df = filtered_df.select(['la_name',comparing])

    
    # Create a figure with a single subplot
    fig, ax = plt.subplots(figsize=(8, 6))

    # Get the values for la1 and la2 and remove any null col values
    la1_val = comparing_df.filter((comparing_df.la_name == la1) & (~col(comparing).isNull())).select(col(comparing)).first()[0]
    la2_val = comparing_df.filter((comparing_df.la_name == la2) & (~col(comparing).isNull())).select(col(comparing)).first()[0]

    # Plot the values
    ax.bar([0.2], la1_val, width=0.2, color='red', label=la1)
    ax.bar([0.6], la2_val, width=0.2, color='green', label=la2)
    ax.set_title(text)
    ax.legend(loc='upper right')
    #plt.xlabel('Local authority')
    plt.ylabel(comparing)
    plt.gca().set_xticklabels([])
    #ax.set_yscale('log')
    #ax.set_ylim(bottom=1)

    return fig



