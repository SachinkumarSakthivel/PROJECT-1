import streamlit as st
import easyocr
import re
import mysql.connector
import pandas as pd
from PIL import Image
mydb=mysql.connector.connect(
    host='localhost',
    user='root',
    password="",
)
print(mydb)
mycursor=mydb.cursor(buffered=True)

# mycursor.execute('create database business')
# mydb.commit()
try:
    mycursor.execute('use business')
    mycursor.execute('create table card_details(name varchar(200),role varchar(200),address varchar(200),website varchar(200) primary key,email varchar(100),mobile varchar(100),pincode varchar(200),company_name varchar(200))')
    mydb.commit()
except:
    print('data exists')  
st.header('Extracting Business Card Data with OCR'.upper())
file=st.sidebar.file_uploader('Upload the Business Card')
if file:
    reader=easyocr.Reader(['th','en'])
    fname=file.name
    path=r"C:\Users\SACHINKUMAR\Downloads"
    
    r=reader.readtext(f"{path}\{fname}")
    st.image(Image.open(f"{path}\{fname}"),width=300)
    for i in r:
        for_mail='.*@.*.com'
        mail=re.match(for_mail,str(i[1]))
        for_web='^ww.*.com'
        web=re.match(for_web,str(i[1]))
        for_pin='\d+'
        pin=re.match(for_pin,str(i[1]))
        for_mob=".*-.*"
        mob=re.match(for_mob,str(i[1]))
        if mail:
            email=str(i[1])
        if web:
            website=str(i[1])
        if pin:
            pincode=str(i[1])
        if mob:
            mobile=str(i[1])
        elif ',' in str(i[1]):
            address=str(i[1])
        else:
            company_type=str(i[1]).strip()
    stand=[]
    for i in r:
        stand.append(str(i[1]))
    name=stand[0]
    role=stand[1]
    if st.button('DISPLAY THE EXTRACTED DATA'):
        st.write('NAME =' , name ),st.write('ROLE =', role ),st.write('ADDRESS = ', address), st.write('WEBSITE = ',website),st.write('EMAIL = ', email),st.write('MOBILE = ', mobile), st.write('PINCODE = ', pincode), st.write('COMPANY = ',company_type)
    sql_but=st.sidebar.button('LOAD INTO SQL')
    if sql_but:
        try:
            mycursor.execute(f"insert into card_details values('{name}','{role}','{address}','{website}','{email}','{mobile}','{pincode}','{company_type}')")
            mydb.commit()
            st.sidebar.write('SUCCESSFULLY LOADED')
        except:
            st.sidebar.write('Data already Exists'.upper())
    
    val=mycursor.execute(f"select * from card_details WHERE name='{name}'")
    final=mycursor.fetchall()
    df=pd.DataFrame(final,columns=['name','role','address','website','Email','Mobile','Pincode','company'])
    no=st.sidebar.radio('SELECT FROM BELOW',('NONE','UPDATE','DELETE'))
    if no=='NONE':
        st.sidebar.write('I HOPE THE DETAILS WERE CORRECT')
    if no=='UPDATE':
        up_value=st.sidebar.selectbox('SELECT ONE TO UPDATE',('ADDRESS','WEBSITE','EMAIL','MOBILE','PINCODE','COMPANY_TYPE'))
        if up_value:
            st.sidebar.write('OOPS! SORRY FOR THE MISTAKE')
            fin=st.sidebar.text_input(f'ENTER NEW {up_value}')
            if fin:
                try:
                    mycursor.execute(f"update card_details set {up_value.lower()}='{fin}' where name='{name}' or email='{email}'")
                    mydb.commit()
                    st.sidebar.write('SUCCESSFULLY UPDATED')
                    sh=st.sidebar.checkbox('SHOW')
                except:
                    st.sidebar.write('SOMETHING GONE WRONG')
                if sh:
                    val=mycursor.execute(f"select * from card_details WHERE name='{name}' or email='{email}'")
                    final=mycursor.fetchall()
                    st.subheader('THE UPDATED VALUES')
                    df=st.dataframe(pd.DataFrame(final,columns=['name','role','address','website','Email','Mobile','Pincode','company']))


    if no=='DELETE':
        de_value=st.sidebar.selectbox('SELECT ONE TO DELETE',('None','NAME','ADDRESS','WEBSITE','EMAIL','MOBILE','PINCODE','COMPANY_TYPE'))
        if de_value is not 'None':
            try:
                mycursor.execute(f"update card_details set {de_value.lower()}=NULL where name='{name}' or email='{email}'")
                st.sidebar.write('SUCCESSFULLY DELETED')
                sho=st.sidebar.checkbox('SHOW')
            except:
                st.sidebar.write('SOMETHING GONE WRONG')
    
            if sho:
                val=mycursor.execute(f"select * from card_details WHERE name='{name}'")
                final=mycursor.fetchall()
                st.subheader('THE UPDATED VALUES')
                df=st.dataframe(pd.DataFrame(final,columns=['name','role','address','website','Email','Mobile','Pincode','company']))

        


    