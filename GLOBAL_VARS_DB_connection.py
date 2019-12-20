import os
import sys
import ast
import pymssql
import GLOBAL_VARS

# FUNCTION TO WRITE TO FILE
def func(path_to_file):
    def write_to_file(args):
        list_a = []
        list_b = []
        for i in args:
            list_a.append(i[1])
            list_b.append(i[0])

        for i in range(len(list_a)):
            FILE.write(""+list_b[i]+" = '"+list_a[i]+"'\n")
            globals()[list_b[i]]= list_a[i]
        FILE.write("\n")

    conn = pymssql.connect("Server Name", "username", "password", "DB Name")

    # conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};Server=(localdb)\MSSQLLocalDB;Integrated Security=true; database = DB Name') 
    cursor = conn.cursor()

    reload_filename = "reload_test_file.config"
    reload_path_to_file = 'path to reload_filename ' + reload_filename

    filename = "GLOBAL_VARS.py"
    path_to_file = 'path to filename’ + filename  
   FILE = open(path_to_file, "a+")
    
    
    
    # random_variables
    cursor.execute("SELECT random_var,variable FROM [DB Name].[dbo].[random_variables]")
    args = cursor.fetchall()
    write_to_file(args)

    # global_path
    cursor.execute("SELECT variable,path FROM [DB Name].[dbo].[global_path]")
    args = cursor.fetchall()
    write_to_file(args)

    # sharepoint_url_path
    cursor.execute("SELECT sharepoint_variable,url_path FROM [DB Name].[dbo].[sharepoint_url_path]")
    args = cursor.fetchall()
    write_to_file(args)

    # kafka_table
    cursor.execute("SELECT kafka_variable,kafka_values FROM [DB Name].[dbo].[kafka_table]")
    args = cursor.fetchall()
    write_to_file(args)

    # random_path
    cursor.execute("SELECT random_variable,path FROM [DB Name].[dbo].[random_path]")
    args = cursor.fetchall()
    write_to_file(args)

    # non_string_datatype
    cursor.execute("SELECT variables,value FROM [DB Name].[dbo].[non_string_datatype] where variables not in ('SHAREPOINT_DICT','RASA_RESPONSE_URLS','SERVICE_REQUIRED','NEW_UI_USERS','LOCAL_APPS','FOLDER_USERS')")
    list_a = []
    list_b = []
    for i in cursor.fetchall():
        list_a.append(i[1])
        list_b.append(i[0])

    for i in range(len(list_a)):
        FILE.write(""+list_b[i]+" = "+list_a[i]+"\n")
        globals()[list_b[i]]= list_a[i]

# END OF WRITE TO FILE FUNCTION

# Checking for 3 conditions *file existance*,*db connection*,*checking if any table has zero records*

def To_Write():
    reload_filename = "reload_test_file.config"
    reload_path_to_file = 'path to reload_filename ' + reload_filename

    filename = "GLOBAL_VARS.py"
    path_to_file = 'path to filename’ + filename  
    # conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};Server=(localdb)\MSSQLLocalDB;Integrated Security=true; database = DB Name')

    # CHECK IF FILE EXISTS
    if os.path.exists(reload_path_to_file):


        try:
            conn = pymssql.connect("Server Name", "username", "password", "DB Name")
            cursor = conn.cursor()

            # CHECK IF TABLE HAS RECORDS
            cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[sharepoint_url_path]")
            if (cursor.fetchall()[0][0]!=0):                
                cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[global_path]")            
                if (cursor.fetchall()[0][0]!=0):
                    cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[kafka_table]")
                    if (cursor.fetchall()[0][0]!=0):
                        cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[random_path]")
                        if (cursor.fetchall()[0][0]!=0):
                            cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[random_variables]")
                            if (cursor.fetchall()[0][0]!=0):
                                cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[non_string_datatype]")
                                if (cursor.fetchall()[0][0]!=0):
                                    cursor.execute ("SELECT count(*) FROM [DB Name].[dbo].[ClientConfiguration]")
                                    if (cursor.fetchall()[0][0]!=0):

                                        func(path_to_file)

                                        cursor.execute("SELECT variables FROM [DB Name].[dbo].[non_string_datatype] where variables in ('LOCAL_APPS','NEW_UI_USERS','FOLDER_USERS')")
                                        data_tuple=cursor.fetchall()

                                        cursor.execute("SELECT clientname,NewUiEnabled,LocalAppsEnabled,FolderEnabled FROM [DB Name].[dbo].[ClientConfiguration] order by id desc")
                                        data_NewUi_CC=cursor.fetchall()
            
                                        if (data_NewUi_CC[0][0] not in GLOBAL_VARS.NEW_UI_USERS) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.LOCAL_APPS) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.FOLDER_USERS):
                                            if (data_NewUi_CC[0][1] == True) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.NEW_UI_USERS):
                                                GLOBAL_VARS.NEW_UI_USERS=GLOBAL_VARS.NEW_UI_USERS+(data_NewUi_CC[0])[:1]
                   # if (data_NewUi_CC[1][1] == True) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.NEW_UI_USERS):
                   #     GLOBAL_VARS.NEW_UI_USERS=GLOBAL_VARS.NEW_UI_USERS+(data_NewUi_CC[1])[:1]
                                            if (data_NewUi_CC[0][2] == True) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.LOCAL_APPS):
                                                GLOBAL_VARS.LOCAL_APPS=GLOBAL_VARS.LOCAL_APPS+(data_NewUi_CC[0])[:1]
                   # if (data_NewUi_CC[1][2] == True) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.LOCAL_APPS):
                    #    GLOBAL_VARS.LOCAL_APPS=GLOBAL_VARS.LOCAL_APPS+(data_NewUi_CC[1])[:1]
                                            if (data_NewUi_CC[0][3] == True) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.FOLDER_USERS):
                                                GLOBAL_VARS.FOLDER_USERS=GLOBAL_VARS.FOLDER_USERS+(data_NewUi_CC[0])[:1]
                   # if (data_NewUi_CC[1][3] == True) and (data_NewUi_CC[0][0] not in GLOBAL_VARS.FOLDER_USERS):
                    #    GLOBAL_VARS.FOLDER_USERS=GLOBAL_VARS.FOLDER_USERS+(data_NewUi_CC[1])[:1]
                                            FILE = open(path_to_file, "w+")
                                                
                                            FILE.write("NEW_UI_USERS="+str(GLOBAL_VARS.NEW_UI_USERS)+"\n")
                                            FILE.write("LOCAL_APPS="+str(GLOBAL_VARS.LOCAL_APPS)+"\n")
                                            FILE.write("FOLDER_USERS="+str(GLOBAL_VARS.FOLDER_USERS)+"\n")

                                            cursor.execute("SELECT clientname,SharepointCache,SharepointURLPath,SharepointQueryPath,RASA_Response_URL,BERTServiceEnabled,AllenServiceRequired FROM [DB Name].[dbo].[ClientConfiguration] order by id desc")
                                            data1=cursor.fetchall()
                                            if (data1[0][0] not in GLOBAL_VARS.SHAREPOINT_DICT.keys()):                                                
                                                list_sharepoint = [data1[0][1],data1[0][2],data1[0][3]]
                                                GLOBAL_VARS.SHAREPOINT_DICT[data1[0][0]]=list_sharepoint
                                                FILE.write("SHAREPOINT_DICT="+str(GLOBAL_VARS.SHAREPOINT_DICT)+"\n")
                                            else:
                                                FILE.write("SHAREPOINT_DICT="+str(GLOBAL_VARS.SHAREPOINT_DICT)+"\n")
                #updating SHAREPOINT_DICT to DB

                                            if (data1[0][0] not in GLOBAL_VARS.RASA_RESPONSE_URLS.keys()):
                                                GLOBAL_VARS.RASA_RESPONSE_URLS[data1[0][0]]=data1[0][4]
                                                FILE.write("RASA_RESPONSE_URLS="+str(GLOBAL_VARS.RASA_RESPONSE_URLS)+"\n")
                                            else:
                                                FILE.write("RASA_RESPONSE_URLS="+str(GLOBAL_VARS.RASA_RESPONSE_URLS)+"\n")
                #updating RASA_RESPONSE_URLS to DB

                                            if (data1[0][0] not in GLOBAL_VARS.SERVICE_REQUIRED.keys()):
                                                list_service_required = [data1[0][5],data1[0][6]]
                                                GLOBAL_VARS.SERVICE_REQUIRED[data1[0][0]]=list_service_required
                                                FILE.write("SERVICE_REQUIRED="+str(GLOBAL_VARS.SERVICE_REQUIRED)+"\n")
                                            else:
                                                FILE.write("SERVICE_REQUIRED="+str(GLOBAL_VARS.SERVICE_REQUIRED)+"\n")
                                            FILE.write("\n")    
                                            func(path_to_file)
                #updating RASA_RESPONSE_URLS to DB
                                        else:
                                            FILE.write("NEW_UI_USERS="+str(GLOBAL_VARS.NEW_UI_USERS)+"\n")
                                            FILE.write("LOCAL_APPS="+str(GLOBAL_VARS.LOCAL_APPS)+"\n")
                                            FILE.write("FOLDER_USERS="+str(GLOBAL_VARS.FOLDER_USERS)+"\n")
                                            FILE.write("SHAREPOINT_DICT="+str(GLOBAL_VARS.SHAREPOINT_DICT)+"\n")
                                            FILE.write("RASA_RESPONSE_URLS="+str(GLOBAL_VARS.RASA_RESPONSE_URLS)+"\n")
                                            FILE.write("SERVICE_REQUIRED="+str(GLOBAL_VARS.SERVICE_REQUIRED)+"\n")
                                            func(path_to_file)

                

        # IF CONNECTION FAILS
        except pymssql.Error:
            print ("ERROR IN CONNECTION")

    # IF FILE IS NOT FOUND        
    else:
        print('NO FILE FOUND')

    # stop writing to the file
    FILE.close()
    conn.close()
    
# END OF PROGRAM








