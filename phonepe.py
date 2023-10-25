from sqlalchemy import create_engine
import pymysql 
# DEFINE THE DATABASE CREDENTIALS
user = 'root'
password = ''
host = '127.0.0.1'
port = 3306
database = 'phonepe'

engine=create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))
con=engine.connect()
import streamlit as st
import pandas as pd
import os
import json
import plotly.express as px
import mysql.connector
from pprint import pprint
from sqlalchemy import text
import matplotlib as plt
import seaborn as sns



path=r"C:\Users\SACHINKUMAR\Downloads\Python introduction\pulse\data\aggregated\transaction\country\india\state"
path1=r"C:\Users\SACHINKUMAR\Downloads\Python introduction\pulse\data\aggregated\user\country\india\state"
path2=r"C:\Users\SACHINKUMAR\Downloads\Python introduction\pulse\data\map\transaction\hover\country\india\state"
path3=r"C:\Users\SACHINKUMAR\Downloads\Python introduction\pulse\data\map\user\hover\country\india\state"
path4=r"C:\Users\SACHINKUMAR\Downloads\Python introduction\pulse\data\top\transaction\country\india\state"
path5=r"C:\Users\SACHINKUMAR\Downloads\Python introduction\pulse\data\top\user\country\india\state"
a_t_data=os.listdir(path)
a_u_data=os.listdir(path1)
m_t_data=os.listdir(path2)
m_u_data=os.listdir(path3)
t_t_data=os.listdir(path4)
t_u_data=os.listdir(path5)

from pprint import pprint
final={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in a_t_data:
    p_s=path+'/'+ i
    data1=os.listdir(p_s)
    for j in (data1):
        p_y=p_s+'/'+j
        data2=os.listdir(p_y)
        for k in (data2):
            data3=open(p_y+'/'+k,'r')
            data4=json.load(data3)
            for z in data4['data']['transactionData']:
                trans_name=z['name']
                trans_amount=z['paymentInstruments'][0]['amount']
                trans_count=z['paymentInstruments'][0]['count']
                final['State'].append(i)
                final['Year'].append(j)
                final['Transacion_count'].append(trans_count)
                final['Transacion_type'].append(trans_name)
                final['Transacion_amount'].append(trans_amount)
                final['Quater'].append(int(k.strip('.json')))
df=pd.DataFrame(final)
#pprint(final)
try:
    df.to_sql("agg_tran",con=engine)
except:
    print("Table already exists")

a_u_data=os.listdir(path1)
final1={'state':[],'Year':[],'Mobile_brand':[],'count':[],'percentage':[], 'response_time':[],'Quater':[]}
for i in a_u_data:
    p_s1=path1+'/'+i
    data1=os.listdir(p_s1)
    for j in (data1):
        p_y1=p_s1+'/'+j
        data2=os.listdir(p_y1)
        for k in (data2):
            d=open(p_y1+'/'+k,'r')
            data4=json.load(d)
            if data4['data']['usersByDevice'] is not None:
                for z in data4['data']['usersByDevice']:
                #  for x in range(0,len(data4['data']['usersByDevice'])):
                      final1['state'].append(i)
                      final1['Year'].append(j)
                      final1['Mobile_brand'].append(z['brand'])
                      final1['count'].append(z['count'])
                      final1['percentage'].append(z['percentage'])
                      final1['response_time'].append(data4['responseTimestamp'])
                      final1['Quater'].append(int(k.strip('.json')))
df1=pd.DataFrame(final1)

try:
    df1.to_sql('agg_user',con=engine)
except Exception as e:
    print(e)

from pprint import pprint
final2={'State':[], 'Year':[],'Transaction_area':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in m_t_data:
    p_s3=path2+'/'+ i
    data1=os.listdir(p_s3)
    for j in (data1):
        p_y3=p_s3+'/'+j
        data2=os.listdir(p_y3)
        for k in (data2):
            data3=open(p_y3+'/'+k,'r')
            data4=json.load(data3)
            for z in data4['data']['hoverDataList']:
                trans_name=z['name']
                trans_amount=z['metric'][0]['amount']
                trans_count=z['metric'][0]['count']
                trans_type=z['metric'][0]['type']
                final2['State'].append(i)
                final2['Year'].append(j)
                final2['Transaction_area'].append(trans_name)
                final2['Transacion_count'].append(trans_count)
                final2['Transacion_type'].append(trans_type)
                final2['Transacion_amount'].append(trans_amount)
                final2['Quater'].append(int(k.strip('.json')))
df2=pd.DataFrame(final2)

try:
    df2.to_sql('map_tran',con=engine)
except Exception as e:
    print(e)

from pprint import pprint
m_u_data=os.listdir(path3)
final3={'District':[],'state':[],'Year':[],'users':[],'Quater':[]}
for i in m_u_data:
    p_s4=path3+'/'+i
    data1=os.listdir(p_s4)
    for j in (data1):
        p_y4=p_s4+'/'+j
        data2=os.listdir(p_y4)
        for k in (data2):
            d=open(p_y4+'/'+k,'r')
            data4=json.load(d)
            if data4['data']['hoverData'] is not None:
                for z in data4['data']['hoverData']:
                    final3['District'].append(z)
                    final3['state'].append(i)
                    final3['Year'].append(j)
                    final3['users'].append(data4['data']['hoverData'][z]['registeredUsers'])
                    final3['Quater'].append(int(k.strip('.json')))
df3=pd.DataFrame(final3)

try:
    df3.to_sql('map_user',con=engine)
    print('done')
except Exception as e:
    print(e)

from pprint import pprint
final4={'State':[], 'Year':[],'Quater':[],'States':[],'District_name':[], 'District_count':[], 'District_amount':[]}
for i in t_t_data:
    p_s5=path4+'/'+ i
    data1=os.listdir(p_s5)
    for j in (data1):
        p_y5=p_s5+'/'+j
        data2=os.listdir(p_y5)
        for k in (data2):
            data3=open(p_y5+'/'+k,'r')
            data4=json.load(data3)
            for z in data4['data']['districts']:
                di_en_name=z['entityName']
                dis_amount=z['metric']['amount']
                dis_count=z['metric']['count']
                stat=data4['data']['states']
                final4['State'].append(i)
                final4['Year'].append(j)
                final4['States'].append(stat)
                final4["District_name"].append(di_en_name)
                final4['District_amount'].append(dis_amount)
                final4['District_count'].append(dis_count)
                final4['Quater'].append(int(k.strip('.json')))
df5=pd.DataFrame(final4)
#pprint(data4)

try:
    df5.to_sql('top_tran_dist',con=engine)
    print('done')
except Exception as e:
    print(e)

from pprint import pprint
final5={'State':[], 'Year':[],'Quater':[],'States':[],'Pincode_name':[], 'Pincode_count':[], 'Pincode_amount':[]}
for i in t_t_data:
    p_s5=path4+'/'+ i
    data1=os.listdir(p_s5)
    for j in (data1):
        p_y5=p_s5+'/'+j
        data2=os.listdir(p_y5)
        for k in (data2):
            data3=open(p_y5+'/'+k,'r')
            data4=json.load(data3)
            for y in data4['data']['pincodes']:
                pin_en_name=y['entityName']
                pin_amount=y['metric']['amount']
                pin_count=y['metric']['count']
                stat=data4['data']['states']
                final5['State'].append(i)
                final5['Year'].append(j)
                final5['States'].append(stat)
                final5['Pincode_name'].append(pin_en_name)
                final5['Pincode_amount'].append(pin_amount)
                final5['Pincode_count'].append(pin_count)
                final5['Quater'].append(int(k.strip('.json')))
df6=pd.DataFrame(final5)

try:
    df6.to_sql('top_tran_pin',con=engine)
    print('done')
except Exception as e:
    print(e)

from pprint import pprint
final6={'State':[], 'Year':[],'Quater':[],'States':[],'Pincode_name':[], 'reg_users':[]}
for i in t_u_data:
    p_s5=path5+'/'+ i
    data1=os.listdir(p_s5)
    for j in (data1):
        p_y5=p_s5+'/'+j
        data2=os.listdir(p_y5)
        for k in (data2):
            data3=open(p_y5+'/'+k,'r')
            data4=json.load(data3)
            for y in data4['data']['pincodes']:
                pin_en_name=y['name']
                users=y['registeredUsers']
                stat=data4['data']['states']
                final6['State'].append(i)
                final6['Year'].append(j)
                final6['States'].append(stat)
                final6['Pincode_name'].append(pin_en_name)
                final6['reg_users'].append(users)
                final6['Quater'].append(int(k.strip('.json')))
df7=pd.DataFrame(final6)

try:
    df7.to_sql('top_user_pin',con=engine)
    print('done')
except Exception as e:
    print(e)

from pprint import pprint
final7={'State':[], 'Year':[],'Quater':[],'States':[],'District_name':[], 'reg_users':[]}
for i in t_u_data:
    p_s5=path5+'/'+ i
    data1=os.listdir(p_s5)
    for j in (data1):
        p_y5=p_s5+'/'+j
        data2=os.listdir(p_y5)
        for k in (data2):
            data3=open(p_y5+'/'+k,'r')
            data4=json.load(data3)
            for y in data4['data']['districts']:
                dis_name=y['name']
                users=y['registeredUsers']
                stat=data4['data']['states']
                final7['State'].append(i)
                final7['Year'].append(j)
                final7['States'].append(stat)
                final7['District_name'].append(dis_name)
                final7['reg_users'].append(users)
                final7['Quater'].append(int(k.strip('.json')))
df8=pd.DataFrame(final7)

try:
    df8.to_sql('top_user_district',con=engine)
    print('done')
except Exception as e:
    print(e)

val=text('select distinct(State) from agg_tran')
res=con.execute(val)
new=pd.DataFrame(res)
n=new['State']

val1=text('select distinct(Mobile_brand) from agg_user')
res1=con.execute(val1)
new1=pd.DataFrame(res1)
n1=new1['Mobile_brand']
st.sidebar.header('PHONEPE PULSE DATA')
st.sidebar.write('Hint: Click the required portion in pie chart')
A=st.sidebar.selectbox(
    'SELECT FROM BELOW',('AGGREGATION','MAP','TOP')
)
B=st.sidebar.selectbox(
    'SELECT',('TRANSACTION','USER')
)

if A=='AGGREGATION' and B=='TRANSACTION':
    d=st.sidebar.selectbox('SELECT FROM BELOW',('TOTAL','STATE WISE'))
    if d=='STATE WISE':
        a=st.sidebar.selectbox('PLEASE SELECT A YEAR',('2018','2019','2020','2021','2022','2023'))
        b=st.sidebar.selectbox('SELECT THE QUARTER',('1','2','3','4'))
        # c=st.sidebar.selectbox('SELECT THE TRANSACTION TYPE',('Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others'))
        e=st.sidebar.selectbox(f'SELECT THE STATE',(n))
        var1=text(f"select * from agg_tran where year={a} and quater={b} and State='{e}'")
        result1=con.execute(var1)
        chart2=(pd.DataFrame(result1,columns=['index','state','year','quater','transaction_type','transaction_count','transaction_amount']))
        st.subheader(e)
        st.bar_chart(chart2,x='transaction_type',y='transaction_amount')
    if d=='TOTAL':
        var=text('select * from agg_tran')
        result=con.execute(var)
        chart1=(pd.DataFrame(result,columns=['index','state','year','quater','transaction_type','transaction_count','transaction_amount']))
        file=px.sunburst(chart1,path=['year','quater','transaction_type','state'],values='transaction_amount',color='state')
        st.subheader('TOTAL DATA OF TRANSACTIONS')
        st.plotly_chart(file)
        st.bar_chart(chart1,x='state',y='transaction_amount')
    
                
if A=='AGGREGATION' and B=='USER':
    d1=st.sidebar.selectbox('SELECT ONE FROM BELOW',('TOTAL DATA','STATE WISE'))
    if d1=='STATE WISE':
        f=st.sidebar.selectbox('PLEASE SELECT A YEAR',('2018','2019','2020','2021','2022','2023'))
        g=st.sidebar.selectbox('SELECT THE QUARTER',('1','2','3','4'))
        # h=st.sidebar.selectbox(f'SELECT THE MOBILE BRAND',(n1))
        e1=st.sidebar.selectbox(f'SELECT THE STATE',(n))
        var2=text(f"select * from agg_user where year={f} and quater={g} and State='{e1}'")
        result4=con.execute(var2)
        chart4=(pd.DataFrame(result4,columns=['index','state','year','mobile','count','percentage','response','quater']))
        st.subheader(e1)
        st.bar_chart(chart4,x='mobile',y='count')

    if d1=='TOTAL DATA':
        var3=text('select * from agg_user')
        result3=con.execute(var3)
        chart3=(pd.DataFrame(result3,columns=['index','state','year','mobile','count','percentage','response','quater']))
        file3=px.sunburst(chart3,path=['year','quater','state','mobile'],values='count',color='state')
        st.subheader('TOTAL USERS DATA')
        st.plotly_chart(file3)
        st.bar_chart(chart3,x='state',y='count')
  


if A=='MAP' and B=='TRANSACTION':
    que=text(f'select State,sum(Transacion_amount) from map_tran group by State')
    fin=con.execute(que)
    dframe=(pd.DataFrame(fin,columns=['state','transaction_amount']))
    #dframe.to_csv('india.csv')
    
   
    dff = pd.read_csv(r"C:\Users\SACHINKUMAR\Downloads\pythonvs\.venv\Include\.vscode\india.csv")

    fig = px.choropleth(
        dff,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='state',
        color='transaction_amount',
        color_continuous_scale='Reds'
    )

    aa=fig.update_geos(fitbounds="locations", visible=False)
    st.header('TOTAL TRANSACTION DATA OF THE COUNTRY')
    st.plotly_chart(aa)


if A=='MAP' and B=='USER':
    que1=text(f'select State,sum(users) from map_user group by State')
    fin1=con.execute(que1)
    dframe1=(pd.DataFrame(fin1,columns=['state','users']))
    # dframe1.to_csv('india1.csv')
    dff1 = pd.read_csv(r"C:\Users\SACHINKUMAR\Downloads\pythonvs\.venv\Include\.vscode\india1.csv")

    fig1 = px.choropleth(
        dff1,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='state',
        color='users',
        color_continuous_scale='Reds'
    )

    aa1=fig1.update_geos(fitbounds="locations", visible=False)
    st.header('TOTAL USERS DATA OF THE COUNTRY')
    st.plotly_chart(aa1)


if A=='TOP' and B=='TRANSACTION':
    p=st.sidebar.selectbox('SELECT FROM BELOW',('DISTRICTS','PINCODE'))
    if p=='DISTRICTS':
        s=st.sidebar.selectbox('SELECT FROM BELOW',('TOTAL','DISTRICT WISE'))
        if s=='DISTRICT WISE':
            r=st.sidebar.selectbox('PLEASE SELECT A YEAR',('2018','2019','2020','2021','2022','2023'))
            q=st.sidebar.selectbox('SELECT THE QUARTER',('1','2','3','4'))
            e=st.sidebar.selectbox(f'SELECT THE STATE',(n))
            query=text(f"select * from top_tran_dist where Year={r} and Quater={q} and State='{e}'")
            val2=con.execute(query)
            chart6=(pd.DataFrame(val2,columns=['index','state','year','quater','states','district_name','district_count','dist_amount']))
            st.subheader(e)
            st.bar_chart(chart6,x='district_name',y='district_count')
        if s=='TOTAL':
            var7=text('select * from top_tran_dist')
            result7=con.execute(var7)
            chart7=(pd.DataFrame(result7,columns=['index','state','year','quater','states','district_name','district_count','district_amount']))
            file7=px.sunburst(chart7,path=['year','quater','state','district_name'],values='district_count',color='state')
            st.subheader('TOP STATES AND THEIR DISTRICTS WITH HIGH TRANSACTIONS DATA')
            st.plotly_chart(file7)
            st.bar_chart(chart7,x='state',y='district_amount')


    if p=='PINCODE':
        P4=st.sidebar.selectbox('SELECT FROM BELOW',('TOTAL DATA','PINCODE WISE'))
        if P4=='PINCODE WISE':
            p2=st.sidebar.selectbox('PLEASE SELECT A YEAR',('2018','2019','2020','2021','2022','2023'))
            p3=st.sidebar.selectbox('SELECT THE QUARTER',('1','2','3','4'))
            P5=st.sidebar.selectbox(f'SELECT THE STATE',(n))
            query2=text(f"select * from top_tran_pin where Year={p2} and Quater={p3} and State='{P5}'")
            val8=con.execute(query2)
            chart8=(pd.DataFrame(val8,columns=['index','state','year','quater','states','pincode','pincode_count','pincode_amount']))
            st.subheader(P5)
            st.bar_chart(chart8,x='pincode',y='pincode_count')
        if P4=='TOTAL DATA':
            var9=text('select * from top_tran_pin')
            result9=con.execute(var9)
            chart9=(pd.DataFrame(result9,columns=['index','state','year','quater','states','pincode','pincode_count','pincode_amount']))
            file9=px.sunburst(chart9,path=['year','quater','state'],values='pincode_count',color='state')
            st.subheader('TOP STATES WITH HIGH TRANSACTIONS DATA')
            st.plotly_chart(file9)
            st.bar_chart(chart9,x='state',y='pincode_count')

if A=='TOP' and B=='USER':
    p6=st.sidebar.selectbox('SELECT FROM BELOW',('DISTRICTS','PINCODE'))
    if p6=='DISTRICTS':
        p9=st.sidebar.selectbox('SELECT FROM BELOW',('TOTAL DATA','DISTRICT WISE'))
        if p9=='DISTRICT WISE':
            p7=st.sidebar.selectbox('PLEASE SELECT A YEAR',('2018','2019','2020','2021','2022','2023'))
            p8=st.sidebar.selectbox('SELECT THE QUARTER',('1','2','3','4'))
            p10=st.sidebar.selectbox(f'SELECT THE STATE',(n))
            query11=text(f"select * from top_user_district where Year={p7} and Quater={p8} and State='{p10}'")
            val11=con.execute(query11)
            chart11=(pd.DataFrame(val11,columns=['index','state','year','quater','states','district_name','users']))
            st.subheader(p10)
            st.bar_chart(chart11,x='district_name',y='users')
        if p9=='TOTAL DATA':
            var12=text('select * from top_user_district')
            result12=con.execute(var12)
            chart12=(pd.DataFrame(result12,columns=['index','state','year','quater','states','district_name','users']))
            file12=px.sunburst(chart12,path=['year','quater','state','district_name'],values='users',color='state')
            st.subheader('TOP STATES AND THEIR DISTRICTS WITH HIGH USERS DATA')
            st.plotly_chart(file12)
            st.bar_chart(chart12,x='state',y='users')


    if p6=='PINCODE':
        p13=st.sidebar.selectbox('SELECT FROM BELOW',('TOTAL DATA','PINCODE WISE'))
        if p13=='PINCODE WISE':
            p7=st.sidebar.selectbox('PLEASE SELECT A YEAR',('2018','2019','2020','2021','2022','2023'))
            p8=st.sidebar.selectbox('SELECT THE QUARTER',('1','2','3','4'))
            p10=st.sidebar.selectbox(f'SELECT THE STATE',(n))
            query13=text(f"select * from top_user_pin where Year={p7} and Quater={p8} and State='{p10}'")
            val13=con.execute(query13)
            chart13=(pd.DataFrame(val13,columns=['index','state','year','quater','states','pincode','users']))
            st.subheader(p10)
            st.bar_chart(chart13,x='pincode',y='users')
        if p13=='TOTAL DATA':
            var14=text('select * from top_user_district')
            result14=con.execute(var14)
            chart14=(pd.DataFrame(result14,columns=['index','state','year','quater','states','pincode','users']))
            file14=px.sunburst(chart14,path=['year','quater','state','pincode'],values='users',color='state')
            st.subheader('TOP STATES AND THEIR PINCODES WITH HIGH USERS DATA')
            st.plotly_chart(file14)
            st.bar_chart(chart14,x='state',y='users')
        
    















