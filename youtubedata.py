import pymongo
from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st
import plotly.express as px


#CONNECTING API KEY

def Api_connect():
    Api_Id="AIzaSyByNBGH4L65jrH1JdOWCjr_xH6g1Y15c40"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


#GET CHANNELS INFORMATION

def get_channel_info(channel_id):
    request=youtube.channels().list(
        part ="snippet,contentDetails,Statistics",id=channel_id)
        
    response=request.execute()

    for i in response['items']:
        data = dict(
                    Channel_Name =i["snippet"]["title"],
                    Channel_Id =i["id"],
                    Subscription_Count=i["statistics"]["subscriberCount"],
                    Views =i["statistics"]["viewCount"],
                    Total_Videos =i["statistics"]["videoCount"],
                    Channel_Description =i["snippet"]["description"],
                    Playlist_Id =i["contentDetails"]["relatedPlaylists"]["uploads"]
                    )
        return data
    

    #get video ids

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
            
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids 


#GET VIDEO INFORMATION
def get_video_info(video_ids):


    video_data=[]

    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data 



#GET COMMENT INFORMATION
def get_comment_info(video_ids):
        Comment_data=[]
        try:
            for video_id in video_ids:
                request=youtube.commentThreads().list(
                                part="snippet",videoId=video_id,
                                maxResults=50
                                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                                Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        
                Comment_data.append(data)
                        
        except:
           pass    
        return Comment_data     


#GET PLAYLIST DETAILS

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:

                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                Published_At=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data 


#MongoDB Connection
client=pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db=client["Youtube_data"]



def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                        "video_information":vi_details,"comment_information":com_details})

    return "upload completed successfully"

def channels_table():

        mydb=pymysql.connect(host="127.0.0.1",user="root",passwd="San@171293",
                                database="youtube_data",
                                port=3306)
        cursor=mydb.cursor()

        drop_query='''drop table if exists channels'''
        cursor.execute(drop_query)
        mydb.commit()

        
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscription_Count bigint,
                                                            Views bigint,
                                                            Total_Videos int, 
                                                            Channel_Description text, 
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

        
                

        ch_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data["channel_information"])
            df=pd.DataFrame(ch_list) 

            
        for index,row in df.iterrows():

                try:
                        
                        insert_query='''insert into channels(Channel_Name,
                                                        Channel_Id,
                                                        Subscription_Count,
                                                        Views,
                                                        Total_Videos,
                                                        Channel_Description,
                                                        Playlist_Id)
                                                                
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                        
                        values=(row['Channel_Name'],
                                row['Channel_Id'],
                                row['Subscription_Count'],
                                row['Views'],
                                row['Total_Videos'],
                                row['Channel_Description'],
                                row['Playlist_Id'])
                        
                        cursor.execute(insert_query,values)
                        mydb.commit()
                except:
                         pass



def playlist_table():
    mydb=pymysql.connect(host="127.0.0.1",user="root",passwd="San@171293",
                        database="youtube_data",
                        port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                Title varchar(100),
                                Channel_Id varchar(100),
                                Channel_Name varchar(100),
                                Video_Count int
                                )'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
       for i in range(len(pl_data["playlist_information"])):
           pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)


    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            Video_Count
                                            )
                                            values(%s,%s,%s,%s,%s)'''
    
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Video_Count']
                )
    
        cursor.execute(insert_query,values)
        mydb.commit()                     


def comments_table():
    mydb=pymysql.connect(host="127.0.0.1",user="root",passwd="San@171293",
                        database="youtube_data",
                        port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(50)
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)


    for index,row in df3.iterrows():
            
            try:
                 
                insert_query='''insert into comments( Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author
                                                    )
                                                
                                                    values(%s,%s,%s,%s)'''

                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author']
                        )
            
                cursor.execute(insert_query,values)
                mydb.commit()   
            except:
                 pass
            

comments_table()         





def videos_table():
    mydb=pymysql.connect(host="127.0.0.1",user="root",passwd="San@171293",
                database="youtube_data",
                port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                Channel_Id varchar(100),
                                                Video_Id varchar(30) primary key,
                                                Title varchar(150),
                                                Tags text,
                                                Thumbnail varchar(200),
                                                Description text, 
                                                Published_Date varchar(200), 
                                                Views bigint,
                                                Likes bigint,
                                                Comments int,
                                                Favourite_Count int,
                                                Definition varchar(10),
                                                Caption_Status varchar(50)
                                                )'''
    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
       for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():

        try:
        
            insert_query='''insert into videos (Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favourite_Count,
                                                Definition,
                                                Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    " ".join(row['Tags']),
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favourite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                    )
            #print(values,len(values))
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
           pass


videos_table()



def tables():
    channels_table()
    playlist_table()
    comments_table()
    videos_table()

    return "Tables created successfully"    


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df


def show_playlist_table():
    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df2 = st.dataframe(com_list)
    return df2


def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df3=st.dataframe(vi_list)
    return df3


#Streamlit Part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":black[Executed Skills]")
    st.caption(":green[API Integration]")
    st.caption(":green[Python Coding]")
    st.caption(":green[Data Collection]")
    st.caption(":green[MongoDB]")
    st.caption(":green[Data Management using MYSQL]")
    st.caption(":green[Data Analysis]")

channel_id=st.text_input("Enter the Youtube Channel ID")

if st.button("Collect and Store Data to MongoDB"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button(":black[Migrate to MYSQL]"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()   




#SQL connection

mydb=pymysql.connect(host="127.0.0.1",user="root",passwd="San@171293",
                        database="youtube_data",
                        port=3306)
cursor=mydb.cursor()

questions=st.selectbox(":red[DATA ANALYSIS]",("1. What are the names of all the videos and their corresponding channels",
                                                "2. Which channels have the most number of videos",
                                                "3. What are the top10 most viewed videos and their respective channels",
                                                "4. How many comments were made on each videos",
                                                "5. Which videos have the highest number of likes",
                                                "6. What is the total number of likes for each video",
                                                "7. What is the total number of views for each channel",
                                                "8. what are the names of all the channel that have published in year 2022",
                                                "9. Which videos have the highest number of comments"))
                                                



if questions == "1. What are the names of all the videos and their corresponding channels":
    query1 = "select Title as videos, Channel_Name as ChannelName from videos"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif questions == "2. Which channels have the most number of videos":
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif questions == "3. What are the top10 most viewed videos and their respective channels":
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos
                        where Views is not null order by Views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif questions == "4. How many comments were made on each videos":
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"])) 


elif questions == "5. Which videos have the highest number of likes":
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos
                       where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"])) 


elif questions == "6. What is the total number of likes for each video":
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif questions == "7. What is the total number of views for each channel":
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))  


elif questions == "8. what are the names of all the channel that have published in year 2022":
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos
                where extract(year from Published_Date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))




    
elif questions == "9. Which videos have the highest number of comments":
    query9 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos
                       where Comments is not null order by Comments desc'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    st.write(pd.DataFrame(t9, columns=['Video Title', 'Channel Name', 'NO Of Comments'])) 



