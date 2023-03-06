import os
import glob
import pandas as pd
import numpy as np

isic=pd.read_csv('https://unstats.un.org/unsd/classifications/Econ/Download/In%20Text/ISIC_Rev_4_english_structure.Txt')

# CoC cleaning
df=pd.read_excel('Data/Raw/chamber_Date/2021.07.12 MIT Emp MRC 2019 (1).xlsx', sheet_name='Emp MR 2019')
df['founded']=pd.to_datetime(df['FECHA_CONSTITUCION'])
df['size']=df['TAMAÑO'].str.split('.').str[0].fillna(0).astype(int)
df['type']=df['ORGANIZACION'].str.split(' ').str[0].fillna(0).astype(int)

df.rename(columns={'NOMBRE':'name','DIRECCIO':'street','LOCALIDAD':'neighbourhood','CIIU_12':'isic'}, inplace=True)
df=df[['name','founded','type','street','neighbourhood','size','isic']]

df['isic']="000"+df['isic'].fillna(0).astype(float).astype(int).astype(str)

df['isic']=df['isic'].str[-4:]

type_dict={2901:'Person',2903:'Limited Company',2916:'SAS',2904:"LLC"}
df['type_lab']=df.replace({'type':type_dict})['type']

def isic_level(df, isic, level):

	subset = isic[isic['Code'].str.len() == level]

	if level==1:
		df['isic_'+str(level)]=df['isic'].str[:2]
		isic['sector']=pd.merge(isic,subset, how='left', on='Code')['Description_y'].ffill()
		merged=pd.merge(df,isic, how='left', left_on='isic_'+str(level),right_on='Code')
		df['isic_'+str(level)+'_lab']=merged['sector']
		df['isic_'+str(level)]=merged['Code']
	else:
		df['isic_'+str(level)]=df['isic'].str[:level]
		df['isic_'+str(level)+'_lab']=pd.merge(df,subset, how='left', left_on='isic_'+str(level),right_on='Code')['Description']
	
	return df

for i in range(1,5):
	print(i)
	df=isic_level(df, isic, i)

df=df.drop(columns={'isic'})
df.to_csv('Data/Clean/CoC.csv', index_label='index')

quit()

#google places cleaning
cwd = os.getcwd()
os.chdir("Data/Raw/amenities_ind_csv_files")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

def consolidate_cols(identifier):
	try:
		cols = [col for col in df.columns if identifier in col]
		df[cols[0]]=df[cols[0]].fillna(df[cols[1]])
	except:
		pass
	if len(cols)==0:
		df[['status','types','perm_closed']]=np.nan
	return df


master=pd.DataFrame()
for i in all_filenames:
	df=pd.read_csv(i)
	name=df.iloc[:, list(df.columns).index('lat')-1].name
	df=df.rename(columns={name:'name'})
	consolidate_cols('status')
	consolidate_cols('perm')
	df['filename']=i.split('.')[0]
	df=df[["name", "lat", "lon","types","status","perm_closed",'filename']]
	master=pd.concat([master,df])

print(master)

os.chdir(cwd)
master.to_csv( "Data/Clean/google_places.csv", index=False, encoding='utf-8-sig')

