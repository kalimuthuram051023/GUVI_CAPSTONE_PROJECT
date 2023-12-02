# GUVI_CAPSTONE_PROJECT\\\ YOUTUBE DATA HARVESTING AND WAREHOUSING USING MONGODB,MYSQL & STREAMLIT

YOUTUBE DATA HARVESTING AND WAREHOUSING USING MONGODB,MYSQL & STREAMLIT:-

Problem Statement:

This project is to build a Streamlit app that permits users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access data like channel information, video details, and user engagement. The app should facilitate storing the data in a MongoDB database and allow users to collect data from up to 10 different channels. Additionally, it should offer the capability to migrate selected channel data from the data lake to a SQL database for further analysis. The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

Technology Stack Used:-

1.Google Client Library

2.Youtube API

3.VS code

4.Python

5.MongoDB

6.MYSQL

7.Streamlit


Approach:-


1.Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.

2.Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python.

3.Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the same data in the database in 3 different collections.

4.Transferring the collected data from multiple channels namely the channels,videos,playlists and comments to a SQL data warehouse, utilizing a SQL database like MySQL for this purpose.

5.Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly given the the foreign and the primary key.

6.The retrieved data is displayed with the help of streamlit application.
