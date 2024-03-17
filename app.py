import requests,json,math
import pandas as pd

url = "https://covid-193.p.rapidapi.com/statistics"

headers = {
	"X-RapidAPI-Key": "6a139b3d89msh87b55137953649ap113f04jsncc85666413b5",
	"X-RapidAPI-Host": "covid-193.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
data = response.json()
data = data.get('response')

covid_data = []
for i in range(0,len(data)):
    data_dict = {}
    data_dict["Date"] = data[i].get('day')
    data_dict["Continent"] = data[i].get('continent')
    data_dict["Country"] = data[i].get('country')
    data_dict["Population"] = data[i].get('population')
    data_dict["Total Tests"] = data[i].get('tests').get('total')
    data_dict["Total Cases"] = data[i].get('cases').get('total')
    data_dict["Active Cases"] = data[i].get('cases').get('active')
    data_dict["Critical Cases"] = data[i].get('cases').get('critical')
    data_dict["Recovered"] = data[i].get('cases').get('recovered')
    data_dict["Total Deaths"] = data[i].get('deaths').get('total')
    covid_data.append(data_dict)

def convert_to_int(x):
    if not isinstance(x,int) and x != "UNKNOWN":
        return int(math.floor(float(x)))
    else:
        return x

def clean_text(x):
    x = x.replace('-',' ')
    return x

def transform_data(df):
    df = df.fillna("UNKNOWN")
    df['Continent'] = df['Continent'].apply(clean_text)
    df['Country'] = df['Country'].apply(clean_text)
    columns_to_convert = ["Population","Total Tests","Total Cases","Active Cases","Critical Cases","Recovered","Total Deaths"]
    for col in columns_to_convert:
        df[col] = df[col].apply(convert_to_int)
    return df

df = pd.DataFrame(covid_data)
df = transform_data(df)

df.to_csv('covid_data.csv',index=False)
# old_df = pd.read_csv("covid_data.csv")
# new_df = pd.concat([df,old_df],ignore_index=True)
# new_df.to_csv('covid_data.csv',index=False)
print(df)
